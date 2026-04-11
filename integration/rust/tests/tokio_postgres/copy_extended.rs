use bytes::Bytes;
use futures_util::{pin_mut, SinkExt};
use rust::setup::connections_tokio;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
use tokio_postgres::CopyInSink;

/// Test COPY FROM STDIN via extended protocol - known to cause connection leaks (#885).
/// After a COPY IN operation, the connection should remain usable.
#[tokio::test]
async fn test_copy_in_extended_protocol() {
    for conn in connections_tokio().await {
        conn.batch_execute(
            "DROP TABLE IF EXISTS test_copy_ext;
             CREATE TABLE test_copy_ext (id BIGINT, value TEXT);",
        )
        .await
        .unwrap();

        // Use text-mode COPY IN via copy_in with the Sink trait
        let sink: CopyInSink<Bytes> = conn
            .copy_in("COPY test_copy_ext (id, value) FROM STDIN")
            .await
            .unwrap();
        pin_mut!(sink);

        let data = Bytes::from("1\thello\n2\tworld\n3\tfoo\n");
        sink.send(data).await.unwrap();
        let rows_written = sink.finish().await.unwrap();
        assert_eq!(rows_written, 3);

        // Verify the connection is still usable after COPY
        let rows = conn
            .query("SELECT COUNT(*)::bigint FROM test_copy_ext", &[])
            .await
            .unwrap();
        let count: i64 = rows[0].get(0);
        assert_eq!(count, 3);

        conn.batch_execute("DROP TABLE IF EXISTS test_copy_ext")
            .await
            .unwrap();
    }
}

/// Test COPY IN with binary format
#[tokio::test]
async fn test_copy_in_binary() {
    for conn in connections_tokio().await {
        conn.batch_execute(
            "DROP TABLE IF EXISTS test_copy_bin;
             CREATE TABLE test_copy_bin (id BIGINT, name TEXT);",
        )
        .await
        .unwrap();

        let sink = conn
            .copy_in("COPY test_copy_bin (id, name) FROM STDIN BINARY")
            .await
            .unwrap();

        let writer = BinaryCopyInWriter::new(sink, &[Type::INT8, Type::TEXT]);
        pin_mut!(writer);

        for i in 0..10_i64 {
            let name = format!("name_{}", i);
            writer
                .as_mut()
                .write(&[&i, &name.as_str()])
                .await
                .unwrap();
        }

        writer.finish().await.unwrap();

        // Verify rows
        let rows = conn
            .query("SELECT COUNT(*)::bigint FROM test_copy_bin", &[])
            .await
            .unwrap();
        let count: i64 = rows[0].get(0);
        assert_eq!(count, 10);

        // Connection should still work after binary COPY
        let rows = conn.query("SELECT 1::bigint", &[]).await.unwrap();
        let one: i64 = rows[0].get(0);
        assert_eq!(one, 1);

        conn.batch_execute("DROP TABLE IF EXISTS test_copy_bin")
            .await
            .unwrap();
    }
}

/// Test repeated COPY IN to check for connection leaks
#[tokio::test]
async fn test_copy_in_repeated_no_leak() {
    for conn in connections_tokio().await {
        conn.batch_execute(
            "DROP TABLE IF EXISTS test_copy_leak;
             CREATE TABLE test_copy_leak (id BIGINT, value TEXT);",
        )
        .await
        .unwrap();

        for iteration in 0..5_i64 {
            let sink: CopyInSink<Bytes> = conn
                .copy_in("COPY test_copy_leak (id, value) FROM STDIN")
                .await
                .unwrap();
            pin_mut!(sink);

            let data = Bytes::from(format!("{}\titeration_{}\n", iteration, iteration));
            sink.send(data).await.unwrap();
            sink.finish().await.unwrap();

            // Verify connection is still usable each time
            let rows = conn
                .query("SELECT COUNT(*)::bigint FROM test_copy_leak", &[])
                .await
                .unwrap();
            let count: i64 = rows[0].get(0);
            assert_eq!(count, iteration + 1);
        }

        conn.batch_execute("DROP TABLE IF EXISTS test_copy_leak")
            .await
            .unwrap();
    }
}

/// Test COPY IN with empty data
#[tokio::test]
async fn test_copy_in_empty() {
    for conn in connections_tokio().await {
        conn.batch_execute(
            "DROP TABLE IF EXISTS test_copy_empty;
             CREATE TABLE test_copy_empty (id BIGINT, value TEXT);",
        )
        .await
        .unwrap();

        let sink: CopyInSink<Bytes> = conn
            .copy_in("COPY test_copy_empty (id, value) FROM STDIN")
            .await
            .unwrap();
        pin_mut!(sink);

        // Don't send any data, just finish
        let rows_written = sink.finish().await.unwrap();
        assert_eq!(rows_written, 0);

        // Connection should still work
        let rows = conn
            .query("SELECT COUNT(*)::bigint FROM test_copy_empty", &[])
            .await
            .unwrap();
        let count: i64 = rows[0].get(0);
        assert_eq!(count, 0);

        conn.batch_execute("DROP TABLE IF EXISTS test_copy_empty")
            .await
            .unwrap();
    }
}
