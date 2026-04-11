use rust::setup::connections_tokio;
use tokio_postgres::NoTls;

/// Test mixing simple protocol (batch_execute) and extended protocol (query with params)
/// in the same session
#[tokio::test]
async fn test_simple_then_extended() {
    for conn in connections_tokio().await {
        // Simple protocol
        conn.batch_execute("SELECT 1").await.unwrap();

        // Extended protocol
        let rows = conn.query("SELECT $1::bigint", &[&42_i64]).await.unwrap();
        let val: i64 = rows[0].get(0);
        assert_eq!(val, 42);
    }
}

/// Test alternating between simple and extended protocol many times
#[tokio::test]
async fn test_alternating_simple_extended() {
    for conn in connections_tokio().await {
        for i in 0..25_i64 {
            // Simple protocol
            conn.batch_execute(&format!("SELECT {}", i)).await.unwrap();

            // Extended protocol
            let rows = conn.query("SELECT $1::bigint", &[&i]).await.unwrap();
            let val: i64 = rows[0].get(0);
            assert_eq!(val, i);
        }
    }
}

/// Test simple protocol DDL followed by extended protocol DML
#[tokio::test]
async fn test_simple_ddl_extended_dml() {
    for conn in connections_tokio().await {
        // DDL via simple protocol
        conn.batch_execute(
            "DROP TABLE IF EXISTS test_mixed_proto;
             CREATE TABLE test_mixed_proto (id BIGINT PRIMARY KEY, name TEXT);"
        ).await.unwrap();

        // DML via extended protocol
        conn.execute(
            "INSERT INTO test_mixed_proto (id, name) VALUES ($1, $2)",
            &[&1_i64, &"hello"],
        ).await.unwrap();

        let rows = conn.query(
            "SELECT name FROM test_mixed_proto WHERE id = $1",
            &[&1_i64],
        ).await.unwrap();
        let name: String = rows[0].get(0);
        assert_eq!(name, "hello");

        // Cleanup via simple protocol
        conn.batch_execute("DROP TABLE IF EXISTS test_mixed_proto").await.unwrap();
    }
}

/// Test: prepare statement, use simple protocol, then use the prepared statement again
#[tokio::test]
async fn test_prepared_then_simple_then_prepared() {
    for conn in connections_tokio().await {
        // Prepare (extended protocol)
        let stmt = conn.prepare("SELECT $1::bigint + 1").await.unwrap();

        // Execute prepared statement
        let rows = conn.query(&stmt, &[&10_i64]).await.unwrap();
        assert_eq!(rows[0].get::<_, i64>(0), 11);

        // Simple protocol in between
        conn.batch_execute("SELECT 1; SELECT 2; SELECT 3;").await.unwrap();

        // Use the prepared statement again
        let rows = conn.query(&stmt, &[&20_i64]).await.unwrap();
        assert_eq!(rows[0].get::<_, i64>(0), 21);
    }
}

/// Test: simple protocol error then extended protocol should work
#[tokio::test]
async fn test_simple_error_then_extended() {
    for conn in connections_tokio().await {
        // Cause error via simple protocol
        let err = conn.batch_execute("SELECT * FROM nonexistent_table_xyz_123").await;
        assert!(err.is_err());

        // Extended protocol should still work
        let rows = conn.query("SELECT $1::bigint", &[&99_i64]).await.unwrap();
        assert_eq!(rows[0].get::<_, i64>(0), 99);
    }
}

/// Test: extended protocol error then simple protocol should work
#[tokio::test]
async fn test_extended_error_then_simple() {
    for conn in connections_tokio().await {
        // Error via extended protocol
        let err = conn.query("SELECT * FROM nonexistent_xyz", &[]).await;
        assert!(err.is_err());

        // Simple protocol should still work
        conn.batch_execute("SELECT 1").await.unwrap();
    }
}

/// Test: transaction started with simple protocol, queries via extended protocol
#[tokio::test]
async fn test_transaction_simple_begin_extended_queries() {
    for conn in connections_tokio().await {
        conn.batch_execute(
            "DROP TABLE IF EXISTS test_mixed_txn;
             CREATE TABLE test_mixed_txn (id BIGINT PRIMARY KEY, val TEXT);"
        ).await.unwrap();

        // BEGIN via simple protocol
        conn.batch_execute("BEGIN").await.unwrap();

        // INSERT via extended protocol
        conn.execute(
            "INSERT INTO test_mixed_txn (id, val) VALUES ($1, $2)",
            &[&1_i64, &"in_txn"],
        ).await.unwrap();

        // SELECT via extended protocol
        let rows = conn.query(
            "SELECT val FROM test_mixed_txn WHERE id = $1",
            &[&1_i64],
        ).await.unwrap();
        assert_eq!(rows[0].get::<_, String>(0), "in_txn");

        // COMMIT via simple protocol
        conn.batch_execute("COMMIT").await.unwrap();

        // Verify data persisted
        let rows = conn.query(
            "SELECT val FROM test_mixed_txn WHERE id = $1",
            &[&1_i64],
        ).await.unwrap();
        assert_eq!(rows[0].get::<_, String>(0), "in_txn");

        conn.batch_execute("DROP TABLE IF EXISTS test_mixed_txn").await.unwrap();
    }
}

/// Test: multiple simple protocol queries (semicolon-separated) followed by extended
#[tokio::test]
async fn test_multi_statement_simple_then_extended() {
    for conn in connections_tokio().await {
        // Multi-statement simple protocol
        conn.batch_execute(
            "SELECT 1; SELECT 2; SELECT 3; SELECT 4; SELECT 5;"
        ).await.unwrap();

        // Extended protocol should still work correctly
        for i in 0..10_i64 {
            let rows = conn.query("SELECT $1::bigint", &[&i]).await.unwrap();
            assert_eq!(rows[0].get::<_, i64>(0), i);
        }
    }
}

/// Test: concurrent connections each mixing simple and extended protocol
#[tokio::test]
async fn test_concurrent_mixed_protocol() {
    let mut tasks = vec![];

    for task_id in 0..10_i64 {
        tasks.push(tokio::spawn(async move {
            let (client, connection) = tokio_postgres::connect(
                "host=127.0.0.1 user=pgdog dbname=pgdog password=pgdog port=6432",
                NoTls,
            )
            .await
            .unwrap();

            tokio::spawn(async move {
                let _ = connection.await;
            });

            for i in 0..5_i64 {
                // Simple
                client.batch_execute("SELECT 1").await.unwrap();

                // Extended
                let val = task_id * 100 + i;
                let rows = client.query("SELECT $1::bigint", &[&val]).await.unwrap();
                assert_eq!(rows[0].get::<_, i64>(0), val);
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
}
