use rust::setup::admin_sqlx;
use rust::utils::{Message, connect};
use bytes::{BufMut, BytesMut, Bytes};
use sqlx::Executor;
use tokio::io::AsyncWriteExt;
use tokio_postgres::NoTls;

/// Helper: create a Bind message for a named prepared statement with no params
fn new_bind(portal: &str, statement: &str) -> Message {
    let mut payload = BytesMut::new();
    // destination portal
    payload.put(portal.as_bytes());
    payload.put_u8(0);
    // source statement
    payload.put(statement.as_bytes());
    payload.put_u8(0);
    // number of parameter format codes
    payload.put_i16(0);
    // number of parameters
    payload.put_i16(0);
    // number of result column format codes
    payload.put_i16(0);

    Message {
        code: 'B',
        payload: payload.freeze(),
    }
}

/// Helper: create an Execute message
fn new_execute(portal: &str, max_rows: i32) -> Message {
    let mut payload = BytesMut::new();
    payload.put(portal.as_bytes());
    payload.put_u8(0);
    payload.put_i32(max_rows); // 0 = no limit

    Message {
        code: 'E',
        payload: payload.freeze(),
    }
}

/// Helper: create a Sync message
fn new_sync() -> Message {
    Message {
        code: 'S',
        payload: Bytes::new(),
    }
}

/// Ensure auth is trust before raw protocol tests
async fn ensure_trust_auth() {
    let admin = admin_sqlx().await;
    admin.execute("SET auth_type TO 'trust'").await.unwrap();
}

/// Test basic Parse/Bind/Execute pipelining: send all messages in one flight
#[tokio::test]
async fn test_pipeline_parse_bind_execute() {
    ensure_trust_auth().await;
    let mut stream = connect().await;

    // Pipeline: Parse + Bind + Execute + Sync all at once
    let parse = Message::new_parse("pipe1", "SELECT 1");
    let bind = new_bind("", "pipe1");
    let execute = new_execute("", 0);
    let sync = new_sync();

    // Build all messages into one buffer and send at once
    let mut buf = BytesMut::new();
    for msg in [&parse, &bind, &execute, &sync] {
        buf.put_u8(msg.code as u8);
        buf.put_i32(msg.payload.len() as i32 + 4);
        buf.put(msg.payload.clone());
    }
    stream.write_all(&buf).await.unwrap();
    stream.flush().await.unwrap();

    // Read responses until we get ReadyForQuery ('Z')
    let mut got_data_row = false;
    let mut got_parse_complete = false;
    let mut got_bind_complete = false;
    let mut got_command_complete = false;

    loop {
        let msg = Message::read(&mut stream).await.unwrap();
        match msg.code {
            '1' => got_parse_complete = true,
            '2' => got_bind_complete = true,
            'D' => got_data_row = true,
            'C' => got_command_complete = true,
            'Z' => break,
            'T' => {} // RowDescription - expected
            _ => {}
        }
    }

    assert!(got_parse_complete, "Missing ParseComplete");
    assert!(got_bind_complete, "Missing BindComplete");
    assert!(got_data_row, "Missing DataRow");
    assert!(got_command_complete, "Missing CommandComplete");
}

/// Test multiple Parse/Bind/Execute sequences pipelined in a single flight
#[tokio::test]
async fn test_pipeline_multiple_statements() {
    ensure_trust_auth().await;
    let mut stream = connect().await;

    // Pipeline 3 different prepared statements in one flight
    let mut buf = BytesMut::new();

    for i in 0..3 {
        let name = format!("multi_{}", i);
        let sql = format!("SELECT {}", i + 10);
        let parse = Message::new_parse(&name, &sql);
        let bind = new_bind("", &name);
        let execute = new_execute("", 0);
        let sync = new_sync();

        for msg in [&parse, &bind, &execute, &sync] {
            buf.put_u8(msg.code as u8);
            buf.put_i32(msg.payload.len() as i32 + 4);
            buf.put(msg.payload.clone());
        }
    }

    stream.write_all(&buf).await.unwrap();
    stream.flush().await.unwrap();

    // We should receive 3 complete cycles (each ending with Z)
    let mut ready_count = 0;
    let mut data_rows = 0;

    loop {
        let msg = Message::read(&mut stream).await.unwrap();
        match msg.code {
            'D' => data_rows += 1,
            'Z' => {
                ready_count += 1;
                if ready_count >= 3 {
                    break;
                }
            }
            _ => {}
        }
    }

    assert_eq!(ready_count, 3, "Expected 3 ReadyForQuery messages");
    assert_eq!(data_rows, 3, "Expected 3 DataRow messages");
}

/// Test pipelining Parse then reusing the statement multiple times
#[tokio::test]
async fn test_pipeline_reuse_statement() {
    ensure_trust_auth().await;
    let mut stream = connect().await;

    let mut buf = BytesMut::new();

    // Parse once
    let parse = Message::new_parse("reuse_stmt", "SELECT 42");
    buf.put_u8(parse.code as u8);
    buf.put_i32(parse.payload.len() as i32 + 4);
    buf.put(parse.payload.clone());

    // Bind + Execute 5 times, each with its own Sync
    for _ in 0..5 {
        let bind = new_bind("", "reuse_stmt");
        let execute = new_execute("", 0);
        let sync = new_sync();

        for msg in [&bind, &execute, &sync] {
            buf.put_u8(msg.code as u8);
            buf.put_i32(msg.payload.len() as i32 + 4);
            buf.put(msg.payload.clone());
        }
    }

    stream.write_all(&buf).await.unwrap();
    stream.flush().await.unwrap();

    // We expect ParseComplete, then 5 cycles of (BindComplete, RowDescription, DataRow, CommandComplete, ReadyForQuery)
    // But the first Sync also produces a ReadyForQuery due to the Parse not having its own Sync.
    // Actually Parse without Sync means the ParseComplete comes before the first Bind response.
    let mut ready_count = 0;
    let mut data_rows = 0;

    loop {
        let msg = Message::read(&mut stream).await.unwrap();
        match msg.code {
            'D' => data_rows += 1,
            'Z' => {
                ready_count += 1;
                if ready_count >= 5 {
                    break;
                }
            }
            'E' => {
                // ErrorResponse
                panic!("Got error response during pipeline reuse test");
            }
            _ => {}
        }
    }

    assert_eq!(data_rows, 5, "Expected 5 DataRow results from reused statement");
}

/// Test pipelining via tokio-postgres (which uses extended protocol pipelining internally)
#[tokio::test]
async fn test_tokio_postgres_pipelining() {
    let (client, connection) = tokio_postgres::connect(
        "host=127.0.0.1 user=pgdog dbname=pgdog password=pgdog port=6432",
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Prepare multiple statements sequentially (each uses extended protocol pipelining)
    let mut stmts = vec![];
    for i in 0..10_i64 {
        let sql = format!("SELECT {}::bigint", i);
        let stmt = client.prepare(&sql).await.unwrap();
        stmts.push((i, stmt));
    }

    // Execute all statements
    for (i, stmt) in &stmts {
        let rows = client.query(stmt, &[]).await.unwrap();
        let val: i64 = rows[0].get(0);
        assert_eq!(val, *i);
    }
}
