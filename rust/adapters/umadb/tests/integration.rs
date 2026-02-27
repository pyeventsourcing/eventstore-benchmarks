use bench_core::adapter::{EventData, EventStoreAdapter, ReadRequest};
use umadb_adapter::UmaDbAdapter;

#[tokio::test]
async fn setup_starts_container_and_accepts_writes() {
    let adapter = UmaDbAdapter::new();
    adapter.setup().await.unwrap();

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

    adapter.teardown().await.unwrap();
}

#[tokio::test]
async fn setup_ping_returns_latency() {
    let adapter = UmaDbAdapter::new();
    adapter.setup().await.unwrap();

    let latency = adapter.ping().await.unwrap();
    assert!(latency.as_millis() < 5000);

    adapter.teardown().await.unwrap();
}
