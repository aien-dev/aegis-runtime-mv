use aegis::durability::DurableRuntime;
use tempfile::tempdir;

#[test]
fn identical_reply_content_collapses_to_one_durable_obligation() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("delivery.sqlite");
    let runtime = DurableRuntime::open(&path).unwrap();
    let first = runtime
        .record_reply("sess/a", "msg:1", "exact reply")
        .unwrap();
    let second = runtime
        .record_reply("sess/a", "msg:1", "exact reply")
        .unwrap();
    assert_eq!(first.id, second.id);
    assert_eq!(runtime.pending_replies().unwrap().len(), 1);
    drop(runtime);
    let reopened = DurableRuntime::open(&path).unwrap();
    assert_eq!(reopened.pending_replies().unwrap().len(), 1);
}
