from pathlib import Path

path = Path("crates/oxide-batch/tests/postgres_custom_leaf_crash_recovery.rs")
text = path.read_text()

text = text.replace(
    "//! PostgreSQL 15/18 real-process SIGKILL and restart evidence for registered custom leaves.",
    "//! `PostgreSQL` 15/18 real-process SIGKILL and restart evidence for registered custom leaves.",
    1,
)
text = text.replace(
    "//! The parent always kills a separate worker process from the outside, inspects durable PostgreSQL state,",
    "//! The parent always kills a separate worker process from the outside, inspects durable `PostgreSQL` state,",
    1,
)

if text.count("Duration::from_secs(60)") != 3:
    raise SystemExit(f"expected three one-minute park intervals, found {text.count('Duration::from_secs(60)')}")
text = text.replace("Duration::from_secs(60)", "Duration::from_mins(1)")

if text.count("Duration::from_secs(21_000)") != 1:
    raise SystemExit("expected one 21_000-second recovery clock")
text = text.replace("Duration::from_secs(21_000)", "Duration::from_mins(350)", 1)

old = '''        CrashPoint::BeforeStateCommit => {
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(final_state.context().and_then(state_value), Some(2));
        }
        CrashPoint::AfterStateCommitBeforeTransition => {
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(final_state.context().and_then(state_value), Some(2));
        }
'''
new = '''        CrashPoint::BeforeStateCommit | CrashPoint::AfterStateCommitBeforeTransition => {
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(final_state.context().and_then(state_value), Some(2));
        }
'''
if text.count(old) != 1:
    raise SystemExit(f"expected duplicate final-state match arms once, found {text.count(old)}")
text = text.replace(old, new, 1)

path.write_text(text)
