//! ADR 0034 S6: split commit/capture/upload/acknowledgement model.
//! Run with RUSTFLAGS='--cfg madsim' WASMKEEP_SIM_SEEDS=200 cargo test
//! -p wasmkeep-sim --locked --test madsim_batched --target-dir target/madsim.
//! Alarm-specific mutations live in madsim_alarm_index, using this same model.

#![cfg(madsim)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "support/batched.rs"]
mod batched;
use batched::{Switch, Violation, Wait, caught};

#[test]
fn batched_invariants_and_non_vacuity() {
    batched::safe(true);
}

#[test]
fn back_to_back_events_stay_within_the_loss_bound() {
    batched::back_to_back_safe();
}
#[test]
fn back_to_back_age_one_tick_over_is_caught() {
    batched::back_to_back_caught(Switch::MaxUnsentPlusOne, Violation::LossBound);
}

#[test]
fn strict_is_the_same_model_with_batching_off() {
    batched::safe(false);
}

#[test]
fn durable_own_segment_with_lower_segment_lost_is_caught() {
    caught(Switch::TwoInFlight, Violation::DurableLost);
}
#[test]
fn flush_from_captured_is_caught() {
    caught(Switch::FlushCaptured, Violation::DurableLost);
}
#[test]
fn two_segments_in_flight_is_caught() {
    caught(Switch::TwoInFlight, Violation::MissingLink);
}
#[test]
fn release_without_barrier_is_caught() {
    caught(Switch::ReleaseWithoutBarrier, Violation::CleanLoss);
}
#[test]
fn snapshot_in_flight_is_caught() {
    caught(Switch::SnapshotInFlight, Violation::NotOwnerPrefix);
}
#[test]
fn reset_from_captured_is_caught() {
    caught(Switch::ResetCaptured, Violation::MissingLink);
}
#[test]
fn reset_in_flight_is_caught() {
    caught(Switch::ResetInFlight, Violation::DeletedReturned);
}
#[test]
fn completion_on_another_hold_is_caught() {
    caught(Switch::CompletionOtherHold, Violation::HeadBackwards);
}
#[test]
fn sidecar_before_acknowledgement_is_caught() {
    caught(Switch::SidecarBeforeAck, Violation::SidecarAhead);
}
#[test]
fn acknowledged_head_moving_back_is_caught() {
    caught(Switch::HeadBackwards, Violation::HeadBackwards);
}
#[test]
fn removing_event_start_rule_breaks_loss_bound() {
    caught(Switch::NoEventStartRule, Violation::LossBound);
}
#[test]
fn acknowledging_late_batched_uploads_is_caught() {
    caught(Switch::AckLate, Violation::DurableLost);
}

#[test]
fn waits_apply_their_own_landed_outcome() {
    // Seed 11246 of the random workload (scenario 21) was the handover's.
    let failed: Vec<_> = [Wait::Handover, Wait::Flush, Wait::Reset, Wait::Cleanup]
        .into_iter()
        .map(|wait| (wait, batched::wait_applies_its_own_landed_outcome(wait)))
        .filter(|(_, out)| !out.violations.is_empty())
        .map(|(wait, out)| (wait, out.violations))
        .collect();
    assert!(failed.is_empty(), "{failed:?}");
}

#[test]
fn healthy_handover_requires_the_whole_window() {
    batched::healthy_window_checks_the_whole_interval();
}

#[test]
fn recording_delete_without_ack_is_caught() {
    caught(Switch::DeleteWithoutAck, Violation::DeletedReturned);
}
#[test]
fn doubling_unsent_age_breaks_loss_bound() {
    caught(Switch::DoubleMaxUnsent, Violation::LossBound);
}
#[test]
fn capture_after_failed_upload_is_caught() {
    caught(Switch::CaptureAfterFailure, Violation::MissingLink);
}
#[test]
fn strict_mutations_are_caught() {
    for (rule, violation) in [
        (Switch::FlushCaptured, Violation::DurableLost),
        (Switch::ReleaseWithoutBarrier, Violation::CleanLoss),
        (Switch::SnapshotInFlight, Violation::NotOwnerPrefix),
        (Switch::ResetCaptured, Violation::MissingLink),
        (Switch::ResetInFlight, Violation::DeletedReturned),
        (Switch::CompletionOtherHold, Violation::HeadBackwards),
        // Sidecar ordering remains a structural check in either mode.
        (Switch::SidecarBeforeAck, Violation::SidecarAhead),
        (Switch::HeadBackwards, Violation::HeadBackwards),
        (Switch::AckLate, Violation::DurableLost),
        (Switch::DeleteWithoutAck, Violation::DeletedReturned),
        (Switch::CaptureAfterFailure, Violation::MissingLink),
    ] {
        batched::caught_mode(rule, violation, false);
    }
}

#[test]
fn switching_to_strict_without_barrier_is_caught() {
    batched::caught_in_scenario(Switch::ModeWithoutBarrier, Violation::StrictAhead, 19);
}

#[test]
fn stalled_age_and_stale_completion_are_found_by_search() {
    for (rule, expected) in [
        (Switch::MaxUnsentPlusOne, Violation::LossBound),
        (Switch::MaxUnsentPlusHalf, Violation::LossBound),
        (Switch::DoubleMaxUnsent, Violation::LossBound),
        (Switch::NoEventStartRule, Violation::LossBound),
        (Switch::CompletionOtherHold, Violation::HeadBackwards),
    ] {
        batched::caught_in_scenario(rule, expected, 21);
    }
}

#[test]
fn slow_healthy_handover_requires_both_windows() {
    batched::caught_in_scenario(Switch::ReleaseWithoutBarrier, Violation::CleanLoss, 30);
}

/// ADR 0034, section 7: a periodic snapshot holds the database at its
/// label, the acknowledged head; one that takes in uncaptured commits is
/// caught.
#[test]
fn snapshot_over_uncaptured_commits_is_caught() {
    caught(Switch::SnapshotUncaptured, Violation::NotOwnerPrefix);
}

/// ADR 0034, section 7, as amended: a periodic snapshot is copied at an
/// idle uploader (the acknowledged head, uncaptured commits left out) and
/// uploads off the mailbox. Through crashes mid-upload, lease cuts,
/// hand-overs and resets, every restore reads a whole snapshot at its
/// label and every acknowledged durable write.
#[test]
fn snapshots_uploading_off_the_mailbox_keep_every_restore_whole() {
    batched::snapshots_safe();
}
#[test]
fn a_snapshot_published_before_its_upload_landed_is_caught() {
    batched::snapshots_caught(Switch::SnapshotPublishedEarly, Violation::NotOwnerPrefix);
}
#[test]
fn collecting_before_the_snapshot_is_acknowledged_is_caught() {
    batched::snapshots_caught(Switch::SnapshotCollectEarly, Violation::MissingLink);
}
#[test]
fn collecting_before_the_snapshot_is_acknowledged_loses_durable_writes() {
    batched::snapshots_caught(Switch::SnapshotCollectEarly, Violation::DurableLost);
}
#[test]
fn a_snapshot_labelled_where_its_upload_lands_is_caught() {
    batched::snapshots_caught(Switch::SnapshotLabelAtLanding, Violation::NotOwnerPrefix);
}
#[test]
fn a_staged_snapshot_over_uncaptured_commits_is_caught() {
    batched::snapshots_caught(Switch::SnapshotUncaptured, Violation::NotOwnerPrefix);
}

/// Fable's interleaving: a periodic snapshot's upload outlives its hold
/// (given up), the actor's next hold on the node deletes it, and the
/// upload lands after the erasure. Caught in the random workload and in
/// the staged one when the reset does not wait.
#[test]
fn a_reset_that_does_not_wait_for_a_snapshot_upload_is_caught() {
    caught(Switch::ResetSkipsSnapshot, Violation::ErasedLeft);
}
#[test]
fn a_staged_reset_that_does_not_wait_for_a_snapshot_upload_is_caught() {
    batched::snapshots_caught(Switch::ResetSkipsSnapshot, Violation::ErasedLeft);
}

/// ADR 0042, section 2: a hibernation's idle snapshot is taken at its
/// barrier, never with a segment in flight; every restore reads a whole
/// snapshot at its label. One labelled at the acknowledged head with a
/// segment in flight, the database as it is, is caught.
#[test]
fn idle_snapshots_at_hibernation_keep_every_restore_whole() {
    batched::idle_snapshots_safe();
}
#[test]
fn an_idle_snapshot_labelled_with_a_segment_in_flight_is_caught() {
    batched::snapshots_caught(Switch::IdleSnapshotInFlight, Violation::NotOwnerPrefix);
}
