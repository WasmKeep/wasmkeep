//! Alarms and the bucket's alarm index across nodes, including late and leftover entries.

use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn an_alarm_set_before_a_startup_error_is_indexed_and_fires_elsewhere() {
    // on-init sets an alarm, then recording that it ran fails. The alarm
    // reaches the bucket either way; it must also be in the alarm index,
    // or no node finds it once a is gone (ADR 0009).
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    wasmkeep_storage::faults::fail_next("set_meta:init_version");
    let r = send(p.a.addr, "GET", "/actor/t/alarm-on-init-1/fired", "").await;
    assert_eq!(
        r.status, 500,
        "the injected fault fails the event: {}",
        r.body
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    p.flaky.down.store(true, Ordering::SeqCst);

    // As in orphaned_alarm_fires: due in 1.5 s, overdue once a's lease ran
    // out; then one request, whose count is read before anything it does
    // could arm an alarm.
    wait_delivered(&p.flaky.inner, &["alarm-on-init-1"]).await;
    let r = send(p.b.addr, "GET", "/actor/t/alarm-on-init-1/fired", "").await;
    assert_eq!(r.node.as_deref(), Some("b"));
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "1"),
        "the alarm set by on-init never fired on b"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_alarm_from_single_node_data_is_indexed_before_its_first_upload() {
    // A node that ran on its own joins a cluster with an actor whose alarm
    // is set. The actor's first upload carries that alarm into the bucket;
    // the alarm index must list it by then, or no node finds it if this
    // one is cut off before its first event ends (ADR 0009).
    let da = tempfile::tempdir().unwrap();
    let alone = node_configured(
        "alone",
        da.path(),
        &(Arc::new(InMemory::new()) as Arc<dyn ObjectStore>),
        Duration::from_secs(30),
        &["t"],
        test_actor_wasm(),
        Duration::from_secs(30),
        "",
        |config| config.cluster = None,
    )
    .await;
    let set = std::time::Instant::now();
    let r = send(alone.addr, "POST", "/actor/t/joined/alarm?in=6000", "").await;
    assert_eq!(r.status, 200, "{}", r.body);
    drop(alone);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // a joins with that data; b shares the bucket.
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_a: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;
    let db = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        da.path(),
        &seen_by_a,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    let b = node_with(
        "b",
        db.path(),
        &shared,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;

    // The first event takes the actor on a (its first upload), and a is
    // cut off from the bucket before the event ends.
    let addr = a.addr;
    let slow =
        tokio::spawn(async move { send(addr, "POST", "/actor/t/joined/sleep?ms=1500", "").await });
    tokio::time::sleep(Duration::from_millis(700)).await;
    flaky.down.store(true, Ordering::SeqCst);
    assert!(
        !actor_keys(&shared, "joined").await.is_empty(),
        "the first upload (with the alarm) landed before a was cut off"
    );
    let _ = slow.await;

    // Due 6 s after it was set, overdue once a's lease ran out: b's sweep
    // delivers it through the index, the only way b can know of it.
    tokio::time::sleep(Duration::from_secs(8).saturating_sub(set.elapsed())).await;
    wait_delivered(&shared, &["joined"]).await;
    let r = send(b.addr, "GET", "/actor/t/joined/fired", "").await;
    assert_eq!(r.node.as_deref(), Some("b"), "{} {}", r.status, r.body);
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "1"),
        "the alarm from single-node data never fired on b"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_alarm_fires_on_another_node_when_its_owner_is_gone() {
    orphaned_alarm_fires(0).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn entries_nobody_can_act_on_do_not_hold_back_the_rest() {
    // More stuck entries (a class no node serves) than one sweep reads,
    // all older than the orphaned alarm: sweeps must get past them.
    orphaned_alarm_fires(2_500).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_bucket_knows_which_actors_have_alarms() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        data.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;

    send(a.addr, "POST", "/actor/t/x/alarm?in=60000", "").await;
    let keys = alarm_index(&store, |k| !k.is_empty()).await;
    // alarms/<tenant>/<due ms, 15 digits>/<class>.t/<name in hex>/
    // <generation, 20 digits>-<nonce in hex>
    assert_eq!(keys.len(), 1, "{keys:?}");
    let parts: Vec<&str> = keys[0].split('/').collect();
    assert_eq!(parts.len(), 6, "{keys:?}");
    assert_eq!(
        (parts[0], parts[1], parts[2].len(), parts[3], parts[4]),
        ("alarms", "default", 15, "t.t", "78"),
        "{keys:?}"
    );
    assert!(parts[2].bytes().all(|b| b.is_ascii_digit()), "{keys:?}");
    let (generation, nonce) = parts[5].split_once('-').unwrap();
    assert!(
        generation.len() == 20 && generation.bytes().all(|b| b.is_ascii_digit()),
        "{keys:?}"
    );
    assert!(
        !nonce.is_empty()
            && nonce
                .bytes()
                .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')),
        "{keys:?}"
    );
    assert_eq!(
        generation.parse::<u64>().unwrap(),
        lease_generation_of_x(&store).await,
        "written under the owner's lease generation"
    );

    // A new time replaces the entry.
    send(a.addr, "POST", "/actor/t/x/alarm?in=90000", "").await;
    let moved = alarm_index(&store, |k| k.len() == 1 && k != keys.as_slice()).await;
    assert_eq!(moved.len(), 1, "{moved:?}");
    assert_ne!(moved, keys);

    send(a.addr, "DELETE", "/actor/t/x/alarm", "").await;
    let cleared = alarm_index(&store, <[String]>::is_empty).await;
    assert!(
        cleared.is_empty(),
        "cleared alarms leave the index: {cleared:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_failing_alarm_is_retried_by_its_owner_not_taken_over() {
    // a's handler fails twice; a retries with backoff (1 s, then 2 s) and
    // hibernates in between, releasing the lease. Its retries are listed in
    // the index at their new times, so no sweep (b's, or a's own) delivers
    // the alarm early at its original, long overdue time: the backoff holds
    // and the actor stays on a.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let a = node_with(
        "a",
        da.path(),
        &store,
        Duration::from_millis(300),
        "t",
        test_actor_wasm(),
    )
    .await;
    let _b = node_with(
        "b",
        db.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;

    send(a.addr, "POST", "/actor/t/x/fail-next?n=2", "").await;
    send(a.addr, "POST", "/actor/t/x/alarm?in=200", "").await;
    // After the first failure the index lists the retry only: the entry
    // for the original time was replaced, not left behind.
    tokio::time::sleep(Duration::from_millis(700)).await;
    let listed = alarm_index(&store, |_| true).await;
    assert_eq!(listed.len(), 1, "one entry, the retry: {listed:?}");
    // Attempts at ~0.2 s and ~1.2 s fail; the next is due at ~3.2 s. At
    // 2.4 s the original time is 2.2 s overdue (past ttl + grace), so a
    // sweep keyed on it would already have run a third attempt.
    tokio::time::sleep(Duration::from_millis(1_700)).await;
    let r = send(a.addr, "GET", "/actor/t/x/attempts", "").await;
    assert_eq!(r.body.trim(), "2", "the backoff held");
    tokio::time::sleep(Duration::from_millis(3_600)).await;
    let r = send(a.addr, "GET", "/actor/t/x/attempts", "").await;
    assert_eq!(r.node.as_deref(), Some("a"), "the actor stayed on a");
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "3"),
        "two failures, one success"
    );
    assert!(alarm_index(&store, <[String]>::is_empty).await.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stale_index_entry_is_removed_when_it_comes_due() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        data.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    // x exists, with no alarm; the index claims one, long overdue.
    send(a.addr, "POST", "/actor/t/x/fired", "").await;
    let due = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        - 10_000;
    {
        use object_store::ObjectStoreExt as _;
        store
            .put(
                &object_store::path::Path::from(format!("alarms/default/{due:015}/t/78")),
                object_store::PutPayload::new(),
            )
            .await
            .unwrap();
    }
    let left = alarm_index(&store, <[String]>::is_empty).await;
    assert!(left.is_empty(), "the stale entry is gone: {left:?}");
    let r = send(a.addr, "GET", "/actor/t/x/attempts", "").await;
    assert_eq!(r.body.trim(), "0", "and no handler ran for it");
}

#[tokio::test(flavor = "multi_thread")]
async fn legacy_and_owned_entries_at_one_time_fire_once_and_are_removed() {
    // A legacy entry (as nodes without tags wrote it) next to the tagged
    // entry of the actor's own alarm, due at the same time: the alarm runs
    // once, and both entries leave the index (the tagged one when the
    // alarm is cleared, the legacy one when a sweep finds it stale).
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        data.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    let due = epoch_ms() + 1_500;
    let r = send(a.addr, "POST", &format!("/actor/t/x/alarm?at={due}"), "").await;
    assert_eq!(r.status, 200);
    wait_indexed_at(&store, due).await;
    {
        use object_store::ObjectStoreExt as _;
        store
            .put(
                &object_store::path::Path::from(format!("alarms/default/{due:015}/t/78")),
                object_store::PutPayload::new(),
            )
            .await
            .unwrap();
    }
    let both = index_entries_for_x(&store).await;
    assert_eq!(both.len(), 2, "legacy and tagged, both due then: {both:?}");

    let started = std::time::Instant::now();
    loop {
        let left = index_entries_for_x(&store).await;
        if left.is_empty() {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "left in the index: {left:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let r = send(a.addr, "GET", "/actor/t/x/fired", "").await;
    assert_eq!(r.body.trim(), "1", "the alarm ran once");
    let r = send(a.addr, "GET", "/actor/t/x/alarm", "").await;
    assert_eq!(r.body.trim(), "none");
}

#[tokio::test(flavor = "multi_thread")]
async fn er04_a_late_delete_after_a_clear_spares_the_new_owners_entry() {
    late_delete_after_takeover(true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn er04_a_late_delete_after_a_move_spares_the_new_owners_entry() {
    late_delete_after_takeover(false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn er04_a_late_delete_of_ones_own_old_entry_spares_the_new_one() {
    // One owner, one generation: p, then t, then back to p. The delete of
    // the first p entry lands after p was indexed again.
    let p = pair(Duration::from_secs(30), Duration::from_secs(30)).await;
    let shared = Arc::clone(&p.flaky.inner);
    let (at_p, at_t) = (epoch_ms() + 5_000, epoch_ms() + 40_000);
    send(p.a.addr, "POST", &format!("/actor/t/x/alarm?at={at_p}"), "").await;
    wait_indexed_at(&shared, at_p).await;
    p.flaky.hold_alarm_deletes.store(true, Ordering::SeqCst);
    send(p.a.addr, "POST", &format!("/actor/t/x/alarm?at={at_t}"), "").await;
    p.flaky.wait_held(1).await;
    send(p.a.addr, "POST", &format!("/actor/t/x/alarm?at={at_p}"), "").await;
    p.flaky.wait_held(2).await;
    wait_indexed_at(&shared, at_p).await;
    p.flaky.release_held_deletes().await;
    assert_eq!(p.flaky.applied.load(Ordering::SeqCst), 2, "both landed");
    let r = send(p.a.addr, "GET", "/actor/t/x/alarm", "").await;
    assert_eq!(r.body.trim(), at_p.to_string(), "the actor's alarm is at p");
    assert!(
        indexed_at(&shared, at_p).await,
        "the late delete removed the entry for p: {:?}",
        index_entries_for_x(&shared).await
    );
    assert!(
        !indexed_at(&shared, at_t).await,
        "the entry for t was removed"
    );

    // a goes before p; b delivers it from the index (one read, as above).
    p.flaky.down.store(true, Ordering::SeqCst);
    wait_delivered(&shared, &["x"]).await;
    let r = send(p.b.addr, "GET", "/actor/t/x/fired", "").await;
    assert_eq!(r.node.as_deref(), Some("b"), "b took x over");
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "1"),
        "the alarm fired on b"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn er04_a_leftover_entry_cannot_bring_back_an_actor_its_alarm_deleted() {
    // The actor's alarm deletes it, delivered for a leftover entry due
    // earlier than the alarm (one an earlier owner failed to remove). The
    // leftover must go with the actor: delivered again, it would restore
    // the deleted actor and run `on-init` on it. Actors named
    // `alarm-on-init…` set an alarm from `on-init`, so such a rebirth would
    // fire one.
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let down = Arc::new(AtomicBool::new(false));
    let seen_by_a: Arc<dyn ObjectStore> =
        Arc::new(Flaky::new(Arc::clone(&shared), Arc::clone(&down)));
    let (da, db) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let a = node_with("a", da.path(), &seen_by_a, idle, "t", test_actor_wasm()).await;
    let b = node_with("b", db.path(), &shared, idle, "t", test_actor_wasm()).await;
    settle().await;

    let name = "alarm-on-init-d";
    let r = send(
        a.addr,
        "POST",
        &format!("/actor/t/{name}/delete-on-alarm"),
        "",
    )
    .await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    let started = std::time::Instant::now();
    let indexed = loop {
        let keys = index_entries_for(&shared, name).await;
        if let [key] = keys.as_slice() {
            break key.clone();
        }
        assert!(started.elapsed() < Duration::from_secs(5), "{keys:?}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // Let the event's upload land, then cut a off: b takes the actor over
    // through the index.
    tokio::time::sleep(Duration::from_millis(300)).await;
    down.store(true, Ordering::SeqCst);

    // The leftover: a tagged entry of a's generation, a second before the
    // alarm.
    let parts: Vec<&str> = indexed.split('/').collect();
    let (due, tag): (u64, &str) = (parts[2].parse().unwrap(), parts[5]);
    let leftover = format!(
        "alarms/default/{:015}/t.t/{}/{}-ab",
        due - 1_000,
        parts[4],
        &tag[..20]
    );
    {
        use object_store::ObjectStoreExt as _;
        shared
            .put(
                &object_store::path::Path::from(leftover),
                object_store::PutPayload::new(),
            )
            .await
            .unwrap();
    }

    // b delivers the leftover first; the alarm, due by then, runs and
    // deletes the actor. Nothing of it may stay in the index.
    wait_unindexed(&shared, name, Duration::from_secs(15)).await;
    // Long enough for any entry left behind to be delivered, the actor
    // restored and its `on-init` alarm (1.5 s) fired.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let left = index_entries_for(&shared, name).await;
    assert!(left.is_empty(), "{left:?}");
    // A request recreates the actor now: nothing of an earlier rebirth.
    let r = send(b.addr, "GET", &format!("/actor/t/{name}/fired"), "").await;
    assert_eq!(r.node.as_deref(), Some("b"));
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "0"),
        "the deleted actor was brought back by its leftover entry"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn er04_an_alarm_the_index_could_not_record_is_not_replicated() {
    // The event that set the alarm cannot read which entry the actor is
    // indexed under, so it cannot index the new alarm. Replicated anyway,
    // the bucket would hold an alarm no other node can find: if this node
    // died, it would never fire. Either the index lists the alarm, or the
    // actor is given up and the bucket does not have it.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        data.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    assert_eq!(
        send(a.addr, "POST", "/actor/t/x/fired", "").await.status,
        200
    );

    // The response goes out before the event finishes: let that finish
    // (it reads the same meta) before arming the fault for the next one.
    tokio::time::sleep(Duration::from_millis(300)).await;
    wasmkeep_storage::faults::fail_next("meta:alarm_indexed");
    let due = epoch_ms() + 60_000;
    let r = send(a.addr, "POST", &format!("/actor/t/x/alarm?at={due}"), "").await;
    assert_eq!(r.status, 200);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let indexed = indexed_at(&store, due).await;
    let r = send(a.addr, "GET", "/actor/t/x/alarm", "").await;
    assert!(
        !indexed && r.body.trim() == "none",
        "the alarm ({}) is kept but not indexed: {:?}",
        r.body.trim(),
        index_entries_for_x(&store).await
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn er04_a_delete_is_replicated_even_if_the_index_bookkeeping_fails() {
    // `delete-self` on an actor with an alarm: recording that no entry is
    // indexed any more fails. Nothing unindexed would be replicated (the
    // deleted actor has no alarm), so the delete must still reach the
    // bucket: given up instead, the actor would come back with its data.
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        data.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    let due = epoch_ms() + 60_000;
    let r = send(a.addr, "POST", &format!("/actor/t/x/alarm?at={due}"), "").await;
    assert_eq!(r.status, 200);
    wait_indexed_at(&store, due).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    wasmkeep_storage::faults::fail_next("set_meta:alarm_indexed");
    assert_eq!(
        send(a.addr, "POST", "/actor/t/x/delete", "").await.status,
        200
    );
    tokio::time::sleep(Duration::from_millis(500)).await;
    let r = send(a.addr, "GET", "/actor/t/x/alarm", "").await;
    assert_eq!(
        (r.status, r.body.trim()),
        (200, "none"),
        "the deleted actor came back with its alarm"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn er04_an_entry_for_a_deleted_actor_is_removed_without_running_it() {
    // A leftover index entry, due already, for an actor deleted since: the
    // sweep delivers it to the owner, which must remove it without
    // restoring the actor (running it would run `on-init`, which writes and
    // replicates, and for `alarm-on-init…` actors sets an alarm again).
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let data = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        data.path(),
        &store,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    let name = "alarm-on-init-z";
    let r = send(a.addr, "POST", &format!("/actor/t/{name}/delete"), "").await;
    assert_eq!(r.status, 200);
    // The on-init alarm's entry goes with the delete.
    wait_unindexed(&store, name, Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let after_delete = actor_keys(&store, name).await;
    assert!(!after_delete.is_empty(), "the delete is in the bucket");

    let hex: String = name.bytes().map(|b| format!("{b:02x}")).collect();
    let leftover = format!(
        "alarms/default/{:015}/t.t/{hex}/{:020}-ab",
        epoch_ms() - 10_000,
        1
    );
    {
        use object_store::ObjectStoreExt as _;
        store
            .put(
                &object_store::path::Path::from(leftover),
                object_store::PutPayload::new(),
            )
            .await
            .unwrap();
    }
    wait_unindexed(&store, name, Duration::from_secs(10)).await;
    // Long enough for a rebirth's `on-init` alarm (1.5 s) to be indexed.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let left = index_entries_for(&store, name).await;
    assert!(left.is_empty(), "{left:?}");
    assert_eq!(
        actor_keys(&store, name).await,
        after_delete,
        "the deleted actor ran again"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn er04_a_late_commit_cannot_bring_back_an_alarm_whose_entry_was_cleared() {
    // a sets an alarm: its index entry lands, the upload of the event does
    // not yet (it is slow), and a is cut off. Another node delivers the
    // entry once overdue, restores the state as it is in the bucket (no
    // alarm yet), and removes the entry as stale. Then a's upload lands:
    // nobody wrote after it, so it is the actor's state after all (a late
    // commit, ADR 0007), with the alarm whose entry is gone. Clearing must
    // wait until the new owner's own upload ends the history, so that a
    // late one can no longer become the head.
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = || {
        Arc::new(Flaky::new(
            Arc::clone(&shared),
            Arc::new(AtomicBool::new(false)),
        ))
    };
    let (fa, fb, fc) = (flaky(), flaky(), flaky());
    let dirs = [(); 3].map(|()| tempfile::tempdir().unwrap());
    let idle = Duration::from_secs(30);
    let start = |id: &'static str, store: &Arc<Flaky>, dir: &Path| {
        let (store, dir) = (Arc::clone(store) as Arc<dyn ObjectStore>, dir.to_owned());
        async move { node_with(id, &dir, &store, idle, "t", test_actor_wasm()).await }
    };
    let a = start("a", &fa, dirs[0].path()).await;
    let b = start("b", &fb, dirs[1].path()).await;
    let c = start("c", &fc, dirs[2].path()).await;
    settle().await;
    let r = send(a.addr, "POST", "/actor/t/x/fired", "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    tokio::time::sleep(Duration::from_millis(300)).await;

    let started = std::time::Instant::now();
    fa.delay_wal_ms.store(5_000, Ordering::SeqCst);
    let due = epoch_ms() + 300;
    let r = send(a.addr, "POST", &format!("/actor/t/x/alarm?at={due}"), "").await;
    assert_eq!((r.status, r.node.as_deref()), (200, Some("a")));
    wait_indexed_at(&shared, due).await;
    // Let the event's upload start (it is held 5 s), then cut a off.
    tokio::time::sleep(Duration::from_millis(200)).await;
    fa.down.store(true, Ordering::SeqCst);

    // b or c delivers the overdue entry and finds it stale.
    wait_unindexed(&shared, "x", Duration::from_secs(10)).await;
    // a's upload lands.
    let landed = Duration::from_millis(6_000).saturating_sub(started.elapsed());
    tokio::time::sleep(landed).await;
    let indexed = !index_entries_for_x(&shared).await.is_empty();

    // Cut the current owner off; the last node restores from the bucket.
    let r = send(b.addr, "GET", "/actor/t/x/fired", "").await;
    let (gone, last) = match r.node.as_deref() {
        Some("b") => (&fb, &c),
        Some("c") => (&fc, &b),
        other => panic!("unexpected owner {other:?}"),
    };
    gone.down.store(true, Ordering::SeqCst);
    let last_id = if std::ptr::eq(last, &b) { "b" } else { "c" };
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let alarm = loop {
        let r = send(last.addr, "GET", "/actor/t/x/alarm", "").await;
        if r.node.as_deref() == Some(last_id) {
            break r.body.trim().to_owned();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{last_id} never took x over"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(
        alarm == "none" || indexed,
        "the bucket commits alarm {alarm} but the index has no entry for it"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn an_entry_for_an_actor_the_bucket_never_had_stays_without_creating_it() {
    // An index entry for an actor with nothing in the bucket: its owner
    // listed an alarm its data already held (single-node data) and died
    // before its first upload landed, or that upload is still on its way
    // and will name the entry (ADR 0009). The entry stays: removing it
    // could leave that alarm unlisted. The actor is not created in the
    // bucket either (that would replace the owner's data when it comes
    // back). And the entry is retried with backoff, not on every sweep.
    let shared: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let flaky = Arc::new(Flaky::new(
        Arc::clone(&shared),
        Arc::new(AtomicBool::new(false)),
    ));
    let seen_by_a: Arc<dyn ObjectStore> = Arc::clone(&flaky) as Arc<dyn ObjectStore>;
    let data = tempfile::tempdir().unwrap();
    let a = node_with(
        "a",
        data.path(),
        &seen_by_a,
        Duration::from_secs(30),
        "t",
        test_actor_wasm(),
    )
    .await;
    settle().await;
    let name = "never";
    let hex: String = name.bytes().map(|b| format!("{b:02x}")).collect();
    let entry = format!(
        "alarms/default/{:015}/t.t/{hex}/{:020}-ab",
        epoch_ms() - 10_000,
        1
    );
    {
        use object_store::ObjectStoreExt as _;
        shared
            .put(
                &object_store::path::Path::from(entry),
                object_store::PutPayload::new(),
            )
            .await
            .unwrap();
    }
    // After the first delivery the node lets the actor's lease go at once
    // (expiry 0), though its idle timeout is 30 s: holding it would route
    // client requests there, and the first would create the actor empty.
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (holder, _) = owner_record(&shared, name).await.unwrap();
    assert_eq!(holder, None, "the actor is still held");
    // Sweeps run every 200 ms here: delivered on each, the lease would be
    // taken again dozens of times in 8 s; with the retry 5 min out
    // (`retry_max`), not at all after the first delivery.
    let before = flaky.lease_gets.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_secs(8)).await;
    let reads = flaky.lease_gets.load(Ordering::SeqCst) - before;
    assert_eq!(
        index_entries_for(&shared, name).await.len(),
        1,
        "the entry was removed while the bucket had nothing for the actor"
    );
    assert!(
        actor_keys(&shared, name).await.is_empty(),
        "the actor was created in the bucket"
    );
    assert!(
        reads <= 12,
        "delivered on every sweep: {reads} lease reads in 8 s"
    );
    drop(a);
}
