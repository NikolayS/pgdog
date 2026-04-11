use rust::setup::connections_tokio;

/// Test connection state after a query error - should still be usable
#[tokio::test]
async fn test_query_after_error() {
    for conn in connections_tokio().await {
        for _ in 0..25 {
            // This should fail - syntax error
            let result = conn.query("SELECT FROM invalid_syntax WHERE", &[]).await;
            assert!(result.is_err());

            // Connection should still work
            let rows = conn.query("SELECT 1::bigint", &[]).await.unwrap();
            let one: i64 = rows[0].get(0);
            assert_eq!(one, 1);
        }
    }
}

/// Test error recovery with prepared statements
#[tokio::test]
async fn test_prepared_after_error() {
    for conn in connections_tokio().await {
        // Prepare a valid statement
        let stmt = conn.prepare("SELECT $1::bigint").await.unwrap();

        // Cause an error
        let err = conn.query("SELECT nonexistent_column FROM nonexistent_table", &[]).await;
        assert!(err.is_err());

        // The previously prepared statement should still work
        let rows = conn.query(&stmt, &[&42_i64]).await.unwrap();
        let result: i64 = rows[0].get(0);
        assert_eq!(result, 42);
    }
}

/// Test error during prepare - connection should recover
#[tokio::test]
async fn test_failed_prepare_recovery() {
    for conn in connections_tokio().await {
        // Prepare an invalid statement
        let result = conn.prepare("SELECT FROM invalid").await;
        assert!(result.is_err());

        // Should be able to prepare a valid statement afterwards
        let stmt = conn.prepare("SELECT $1::bigint").await.unwrap();
        let rows = conn.query(&stmt, &[&99_i64]).await.unwrap();
        let result: i64 = rows[0].get(0);
        assert_eq!(result, 99);
    }
}

/// Test type mismatch error recovery
#[tokio::test]
async fn test_type_error_recovery() {
    for conn in connections_tokio().await {
        let stmt = conn.prepare("SELECT $1::bigint").await.unwrap();

        // Execute with correct type
        let rows = conn.query(&stmt, &[&1_i64]).await.unwrap();
        assert_eq!(rows[0].get::<_, i64>(0), 1_i64);

        // Now cause a division by zero error
        let err = conn.query("SELECT 1/0", &[]).await;
        assert!(err.is_err());

        // The statement should still work after the error
        let rows = conn.query(&stmt, &[&2_i64]).await.unwrap();
        assert_eq!(rows[0].get::<_, i64>(0), 2_i64);
    }
}

/// Test multiple sequential errors followed by recovery
#[tokio::test]
async fn test_multiple_errors_then_recovery() {
    for conn in connections_tokio().await {
        // Multiple errors in a row
        for _ in 0..10 {
            let _ = conn.query("INVALID SQL", &[]).await;
        }

        // Should still work
        let rows = conn.query("SELECT 42::bigint", &[]).await.unwrap();
        let result: i64 = rows[0].get(0);
        assert_eq!(result, 42);
    }
}

/// Test error inside a transaction - should be able to rollback and continue
#[tokio::test]
async fn test_error_in_transaction_recovery() {
    for conn in connections_tokio().await {
        conn.batch_execute("BEGIN").await.unwrap();

        // Cause an error inside the transaction
        let err = conn.query("SELECT 1/0", &[]).await;
        assert!(err.is_err());

        // Transaction is now in error state, ROLLBACK should work
        conn.batch_execute("ROLLBACK").await.unwrap();

        // Connection should be fully usable again
        let rows = conn.query("SELECT 1::bigint", &[]).await.unwrap();
        assert_eq!(rows[0].get::<_, i64>(0), 1);
    }
}

/// Test batch_execute error recovery
#[tokio::test]
async fn test_batch_execute_error_recovery() {
    for conn in connections_tokio().await {
        let err = conn
            .batch_execute("SELECT * FROM completely_nonexistent_table_xyz")
            .await;
        assert!(err.is_err());

        // Should still work
        conn.batch_execute("SELECT 1").await.unwrap();
        let rows = conn.query("SELECT 2::bigint", &[]).await.unwrap();
        assert_eq!(rows[0].get::<_, i64>(0), 2);
    }
}
