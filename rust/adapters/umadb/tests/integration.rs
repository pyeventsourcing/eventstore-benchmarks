use bench_core::adapter::{ConnectionParams, EventData, EventStoreAdapter, ReadRequest};
use bench_testcontainers::umadb::{UmaDb, UMADB_PORT};
use testcontainers::runners::AsyncRunner;
use umadb_adapter::UmaDbAdapter;

#[tokio::test]
async fn append_and_read() {
    let container = UmaDb.start().await.unwrap();
    let host_port = container.get_host_port_ipv4(UMADB_PORT).await.unwrap();
    let uri = format!("http://localhost:{}", host_port);

    let adapter = UmaDbAdapter::new();
    adapter
        .connect(&ConnectionParams {
            uri,
            options: Default::default(),
        })
        .await
        .unwrap();

    adapter
        .append(EventData {
            stream: "test-stream".to_string(),
            event_type: "TestEvent".to_string(),
            payload: b"hello".to_vec(),
            tags: vec![],
        })
        .await
        .unwrap();

    let events = adapter
        .read(ReadRequest {
            stream: "test-stream".to_string(),
            from_offset: None,
            limit: Some(10),
        })
        .await
        .unwrap();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "TestEvent");
    assert_eq!(events[0].payload, b"hello");
}

#[tokio::test]
async fn ping() {
    let container = UmaDb.start().await.unwrap();
    let host_port = container.get_host_port_ipv4(UMADB_PORT).await.unwrap();
    let uri = format!("http://localhost:{}", host_port);

    let adapter = UmaDbAdapter::new();
    adapter
        .connect(&ConnectionParams {
            uri,
            options: Default::default(),
        })
        .await
        .unwrap();

    let latency = adapter.ping().await.unwrap();
    assert!(latency.as_millis() < 5000);
}
