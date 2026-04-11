use tokio::spawn;
use tokio_postgres::NoTls;

/// Test multiple connections preparing and executing statements simultaneously
#[tokio::test]
async fn test_concurrent_prepared_statements() {
    let mut tasks = vec![];

    for task_id in 0..10_i64 {
        tasks.push(spawn(async move {
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

            // Each task prepares a different statement
            let stmt = client
                .prepare(&format!(
                    "SELECT $1::bigint + {}",
                    task_id
                ))
                .await
                .unwrap();

            for i in 0..20_i64 {
                let rows = client.query(&stmt, &[&i]).await.unwrap();
                let result: i64 = rows[0].get(0);
                assert_eq!(result, i + task_id);
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
}

/// Test concurrent prepare of the SAME statement from different connections
#[tokio::test]
async fn test_concurrent_same_prepared_statement() {
    let mut tasks = vec![];

    for _ in 0..10 {
        tasks.push(spawn(async move {
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

            // All tasks prepare the exact same statement
            let stmt = client
                .prepare("SELECT $1::bigint * 2")
                .await
                .unwrap();

            for i in 0..20_i64 {
                let rows = client.query(&stmt, &[&i]).await.unwrap();
                let result: i64 = rows[0].get(0);
                assert_eq!(result, i * 2);
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
}

/// Test preparing many different statements on the same connection concurrently
/// (tokio-postgres allows pipelining prepare requests)
#[tokio::test]
async fn test_many_prepared_statements_single_conn() {
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

    // Prepare many different statements
    let mut stmts = vec![];
    for i in 0..50 {
        let stmt = client
            .prepare(&format!("SELECT $1::bigint + {}", i))
            .await
            .unwrap();
        stmts.push((i, stmt));
    }

    // Execute them all
    for (offset, stmt) in &stmts {
        let val = 100_i64;
        let rows = client.query(stmt, &[&val]).await.unwrap();
        let result: i64 = rows[0].get(0);
        assert_eq!(result, val + *offset as i64);
    }
}

/// Test interleaving prepare and execute across concurrent tasks on different connections
#[tokio::test]
async fn test_interleaved_prepare_execute() {
    let mut tasks = vec![];

    for task_id in 0..5_i64 {
        tasks.push(spawn(async move {
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

            // Interleave: prepare stmt A, prepare stmt B, execute A, execute B
            let stmt_a = client
                .prepare(&format!("SELECT $1::bigint + {}", task_id))
                .await
                .unwrap();
            let stmt_b = client
                .prepare(&format!("SELECT $1::text || '{}'", task_id))
                .await
                .unwrap();

            for i in 0..10_i64 {
                let rows_a = client.query(&stmt_a, &[&i]).await.unwrap();
                let result_a: i64 = rows_a[0].get(0);
                assert_eq!(result_a, i + task_id);

                let text = format!("val_{}", i);
                let rows_b = client.query(&stmt_b, &[&text]).await.unwrap();
                let result_b: String = rows_b[0].get(0);
                assert_eq!(result_b, format!("val_{}{}", i, task_id));
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
}
