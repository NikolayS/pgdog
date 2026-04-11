use rust::setup::connections_tokio;

/// Test large bytea payloads via binary protocol
#[tokio::test]
async fn test_large_bytea() {
    for conn in connections_tokio().await {
        // 1MB payload
        let data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

        let rows = conn
            .query("SELECT $1::bytea", &[&data])
            .await
            .unwrap();
        let result: Vec<u8> = rows[0].get(0);
        assert_eq!(result.len(), data.len());
        assert_eq!(result, data);
    }
}

/// Test multiple large bytea values in one query
#[tokio::test]
async fn test_multiple_large_bytea() {
    for conn in connections_tokio().await {
        let data1: Vec<u8> = vec![0xAA; 100_000];
        let data2: Vec<u8> = vec![0xBB; 100_000];

        let rows = conn
            .query("SELECT $1::bytea, $2::bytea", &[&data1, &data2])
            .await
            .unwrap();
        let result1: Vec<u8> = rows[0].get(0);
        let result2: Vec<u8> = rows[0].get(1);
        assert_eq!(result1, data1);
        assert_eq!(result2, data2);
    }
}

/// Test large text payload
#[tokio::test]
async fn test_large_text() {
    for conn in connections_tokio().await {
        let text: String = "A".repeat(1_000_000);

        let rows = conn
            .query("SELECT $1::text", &[&text])
            .await
            .unwrap();
        let result: String = rows[0].get(0);
        assert_eq!(result.len(), text.len());
        assert_eq!(result, text);
    }
}

/// Test bytea round-trip through INSERT/SELECT
#[tokio::test]
async fn test_bytea_insert_select() {
    for conn in connections_tokio().await {
        conn.batch_execute(
            "DROP TABLE IF EXISTS test_bytea_large;
             CREATE TABLE test_bytea_large (id BIGINT PRIMARY KEY, data BYTEA);",
        )
        .await
        .unwrap();

        let data: Vec<u8> = (0..500_000).map(|i| (i % 256) as u8).collect();

        conn.execute(
            "INSERT INTO test_bytea_large (id, data) VALUES ($1, $2)",
            &[&1_i64, &data],
        )
        .await
        .unwrap();

        let rows = conn
            .query("SELECT data FROM test_bytea_large WHERE id = $1", &[&1_i64])
            .await
            .unwrap();
        let result: Vec<u8> = rows[0].get(0);
        assert_eq!(result, data);

        conn.batch_execute("DROP TABLE IF EXISTS test_bytea_large")
            .await
            .unwrap();
    }
}

/// Test sending many medium-sized bytea values rapidly
#[tokio::test]
async fn test_rapid_bytea_queries() {
    for conn in connections_tokio().await {
        for i in 0..50 {
            let size = (i + 1) * 10_000;
            let data: Vec<u8> = (0..size).map(|j| (j % 256) as u8).collect();

            let rows = conn
                .query("SELECT $1::bytea", &[&data])
                .await
                .unwrap();
            let result: Vec<u8> = rows[0].get(0);
            assert_eq!(result.len(), data.len());
        }
    }
}
