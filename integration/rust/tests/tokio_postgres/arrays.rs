use rust::setup::connections_tokio;

/// Test INT[] array type handling via binary protocol
#[tokio::test]
async fn test_int_array() {
    for conn in connections_tokio().await {
        let values: Vec<i32> = vec![1, 2, 3, 4, 5];
        let rows = conn
            .query("SELECT $1::int[]", &[&values])
            .await
            .unwrap();
        let result: Vec<i32> = rows[0].get(0);
        assert_eq!(result, values);
    }
}

/// Test BIGINT[] array type
#[tokio::test]
async fn test_bigint_array() {
    for conn in connections_tokio().await {
        let values: Vec<i64> = vec![100, 200, 300, i64::MAX, i64::MIN];
        let rows = conn
            .query("SELECT $1::bigint[]", &[&values])
            .await
            .unwrap();
        let result: Vec<i64> = rows[0].get(0);
        assert_eq!(result, values);
    }
}

/// Test TEXT[] array type
#[tokio::test]
async fn test_text_array() {
    for conn in connections_tokio().await {
        let values: Vec<String> = vec!["hello".into(), "world".into(), "foo bar".into()];
        let rows = conn
            .query("SELECT $1::text[]", &[&values])
            .await
            .unwrap();
        let result: Vec<String> = rows[0].get(0);
        assert_eq!(result, values);
    }
}

/// Test empty arrays
#[tokio::test]
async fn test_empty_array() {
    for conn in connections_tokio().await {
        let values: Vec<i32> = vec![];
        let rows = conn
            .query("SELECT $1::int[]", &[&values])
            .await
            .unwrap();
        let result: Vec<i32> = rows[0].get(0);
        assert_eq!(result, values);
    }
}

/// Test BOOL[] array type
#[tokio::test]
async fn test_bool_array() {
    for conn in connections_tokio().await {
        let values: Vec<bool> = vec![true, false, true, true, false];
        let rows = conn
            .query("SELECT $1::bool[]", &[&values])
            .await
            .unwrap();
        let result: Vec<bool> = rows[0].get(0);
        assert_eq!(result, values);
    }
}

/// Test FLOAT8[] array type
#[tokio::test]
async fn test_float_array() {
    for conn in connections_tokio().await {
        let values: Vec<f64> = vec![1.1, 2.2, 3.3, 0.0, -1.5];
        let rows = conn
            .query("SELECT $1::float8[]", &[&values])
            .await
            .unwrap();
        let result: Vec<f64> = rows[0].get(0);
        assert_eq!(result, values);
    }
}

/// Test arrays with NULL elements
#[tokio::test]
async fn test_array_with_nulls() {
    for conn in connections_tokio().await {
        let values: Vec<Option<i32>> = vec![Some(1), None, Some(3), None, Some(5)];
        let rows = conn
            .query("SELECT $1::int[]", &[&values])
            .await
            .unwrap();
        let result: Vec<Option<i32>> = rows[0].get(0);
        assert_eq!(result, values);
    }
}

/// Test array in a prepared statement executed multiple times
#[tokio::test]
async fn test_array_prepared() {
    for conn in connections_tokio().await {
        let stmt = conn.prepare("SELECT $1::int[]").await.unwrap();
        for i in 0..10 {
            let values: Vec<i32> = (0..i).collect();
            let rows = conn.query(&stmt, &[&values]).await.unwrap();
            let result: Vec<i32> = rows[0].get(0);
            assert_eq!(result, values);
        }
    }
}

/// Test multi-dimensional concept: array of arrays via text representation
#[tokio::test]
async fn test_large_array() {
    for conn in connections_tokio().await {
        let values: Vec<i32> = (0..1000).collect();
        let rows = conn
            .query("SELECT $1::int[]", &[&values])
            .await
            .unwrap();
        let result: Vec<i32> = rows[0].get(0);
        assert_eq!(result, values);
    }
}
