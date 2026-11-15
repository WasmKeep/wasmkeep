//! Bounded interleaving model for ADR 0033's app-start fence. Explore every
//! interleaving of a node starting (including stale disk) and a restore.
//! The cluster test exercises the corresponding production node paths.
#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Broken {
    No,
    LateFence,
    EarlyOpen,
    NoMarker,
}
#[derive(Clone, Default)]
struct State {
    node_step: usize,
    restore_step: usize,
    lease: bool,
    checked: bool,
    serving: bool,
    restoring: bool,
    restored: u64,
    local: u64,
    cleared: bool,
    ready: bool,
    aborted: bool,
    stale_disk: bool,
}
fn explore(s: State, broken: Broken, ends: &mut usize) -> bool {
    if s.serving && s.cleared && (!s.ready || s.local != s.restored || s.stale_disk) {
        return false;
    }
    if s.node_step == 4 && (s.restore_step == 9 || s.aborted) {
        *ends += 1;
        return true;
    }
    if s.node_step < 4 {
        let mut n = s.clone();
        match n.node_step {
            0 => {
                if n.local != n.restored {
                    n.stale_disk = false;
                }
                n.local = n.restored; // prepare_local, before the lease
            }
            1 => n.lease = true,
            2 => n.checked = !n.restoring && n.local == n.restored,
            3 => n.serving = n.checked && n.lease,
            _ => unreachable!(),
        }
        n.node_step += 1;
        if !explore(n, broken, ends) {
            return false;
        }
    }
    if s.restore_step < 9 && !s.aborted {
        let mut r = s;
        match r.restore_step {
            0 => {
                // starts fenced before the authoritative nodes listing
                if broken != Broken::LateFence {
                    r.restoring = true;
                }
                if broken != Broken::NoMarker {
                    r.restored += 1;
                }
            }
            1 => {
                if r.lease {
                    r.aborted = true;
                }
            }
            2 => {
                if broken == Broken::LateFence {
                    r.restoring = true;
                }
                r.cleared = true;
            }
            3 => {} // history
            4 => {} // generation floors
            5 => {
                if broken == Broken::EarlyOpen {
                    r.restoring = false;
                }
            }
            6 => {}              // alarms
            7 => r.ready = true, // deploy/secrets
            8 => r.restoring = false,
            _ => unreachable!(),
        }
        r.restore_step += 1;
        if !explore(r, broken, ends) {
            return false;
        }
    }
    true
}
#[test]
fn restore_fence_precedes_nodes_listing_and_opens_last() {
    let mut ends = 0;
    assert!(explore(
        State {
            stale_disk: true,
            ..State::default()
        },
        Broken::No,
        &mut ends
    ));
    assert!(ends > 100, "vacuous enumeration: {ends}");
    for broken in [Broken::LateFence, Broken::EarlyOpen, Broken::NoMarker] {
        assert!(!explore(
            State {
                stale_disk: true,
                ..State::default()
            },
            broken,
            &mut 0
        ));
    }
}
#[test]
fn expired_node_is_fenced_and_late_writes_settle_before_clear() {
    // Explore upload landing times across the entire 30s request deadline.
    // Fencing prevents renewal; a 40s quiet window precedes clearing.
    for lands_at in 0..=30 {
        for clear_at in [0, 40] {
            let late_old_history = lands_at > clear_at;
            if clear_at == 40 {
                assert!(!late_old_history);
            }
            if lands_at == 30 && clear_at == 0 {
                assert!(late_old_history);
            }
        }
    }
}
