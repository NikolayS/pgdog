use tokio::spawn;
use tokio_postgres::NoTls;

/// Test rapid connect/disconnect cycles - stress test connection handling
#[tokio::test]
async fn test_rapid_connect_disconnect() {
    // Sequentially connect and immediately disconnect 50 times
    for _ in 0..50 {
        let (client, connection) = tokio_postgres::connect(
            "host=127.0.0.1 user=pgdog dbname=pgdog password=pgdog port=6432",
            NoTls,
        )
        .await
        .unwrap();

        let handle = tokio::spawn(async move {
            let _ = connection.await;
        });

        // Immediately drop the client (disconnect)
        drop(client);
        let _ = handle.await;
    }
}

/// Test rapid connect, single query, then disconnect
#[tokio::test]
async fn test_rapid_connect_query_disconnect() {
    for i in 0..50_i64 {
        let (client, connection) = tokio_postgres::connect(
            "host=127.0.0.1 user=pgdog dbname=pgdog password=pgdog port=6432",
            NoTls,
        )
        .await
        .unwrap();

        tokio::spawn(async move {
            let _ = connection.await;
        });

        let rows = client.query("SELECT $1::bigint", &[&i]).await.unwrap();
        let val: i64 = rows[0].get(0);
        assert_eq!(val, i);

        drop(client);
    }
}

/// Test concurrent rapid connect/disconnect - many connections at once
#[tokio::test]
async fn test_concurrent_rapid_connect_disconnect() {
    let mut tasks = vec![];

    for task_id in 0..20_i64 {
        tasks.push(spawn(async move {
            for j in 0..5_i64 {
                let (client, connection) = tokio_postgres::connect(
                    "host=127.0.0.1 user=pgdog dbname=pgdog password=pgdog port=6432",
                    NoTls,
                )
                .await
                .unwrap();

                tokio::spawn(async move {
                    let _ = connection.await;
                });

                let val = task_id * 100 + j;
                let rows = client.query("SELECT $1::bigint", &[&val]).await.unwrap();
                let result: i64 = rows[0].get(0);
                assert_eq!(result, val);

                drop(client);
            }
        }));
    }

    for task in tasks {
        task.await.unwrap();
    }
}

/// Test connect then immediate disconnect without any query (potential resource leak)
#[tokio::test]
async fn test_connect_no_query_disconnect() {
    let mut handles = vec![];

    for _ in 0..30 {
        handles.push(spawn(async {
            let (client, connection) = tokio_postgres::connect(
                "host=127.0.0.1 user=pgdog dbname=pgdog password=pgdog port=6432",
                NoTls,
            )
            .await
            .unwrap();

            let conn_handle = tokio::spawn(async move {
                let _ = connection.await;
            });

            // Drop immediately without doing anything
            drop(client);
            let _ = conn_handle.await;
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // After all rapid disconnects, verify the proxy still works
    let (client, connection) = tokio_postgres::connect(
        "host=127.0.0.1 user=pgdog dbname=pgdog password=pgdog port=6432",
        NoTls,
    )
    .await
    .unwrap();

    tokio::spawn(async move {
        let _ = connection.await;
    });

    let rows = client.query("SELECT 1::bigint", &[]).await.unwrap();
    let val: i64 = rows[0].get(0);
    assert_eq!(val, 1);
}

/// Test rapid connect/disconnect with prepared statements (connection may be returned
/// to pool with stale prepared statement state)
#[tokio::test]
async fn test_rapid_connect_with_prepared() {
    for i in 0..20_i64 {
        let (client, connection) = tokio_postgres::connect(
            "host=127.0.0.1 user=pgdog dbname=pgdog password=pgdog port=6432",
            NoTls,
        )
        .await
        .unwrap();

        tokio::spawn(async move {
            let _ = connection.await;
        });

        // Prepare and execute
        let stmt = client.prepare("SELECT $1::bigint * 2").await.unwrap();
        let rows = client.query(&stmt, &[&i]).await.unwrap();
        let result: i64 = rows[0].get(0);
        assert_eq!(result, i * 2);

        drop(client);
    }
}
