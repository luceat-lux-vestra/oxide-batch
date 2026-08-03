//! What a third-party implementer sees when they get it wrong.
//!
//! Generic contracts are cheap to design and expensive to use if the errors
//! are unreadable, so the ergonomics review pins the two mistakes an
//! implementer will actually make. Both assertions are on wording, which is
//! the point: a future change that degrades these messages fails here rather
//! than in someone's editor.

#![allow(clippy::expect_used)]

use std::path::PathBuf;
use std::process::Command;

fn compile_fixture(name: &str) -> String {
    let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/ui")
        .join(name);
    let output = Command::new("rustc")
        .args(["--edition=2024", "--crate-type=lib"])
        .arg(&fixture)
        .arg("--out-dir")
        .arg(std::env::temp_dir())
        .output()
        .expect("rustc must run");

    assert!(
        !output.status.success(),
        "{name} was expected to fail compilation"
    );
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn a_missing_impl_reports_the_contract_in_oxidebatch_terms() {
    let stderr = compile_fixture("missing_impl.rs");

    // The `#[diagnostic::on_unimplemented]` wording replaces the bare E0277
    // headline, names both the component and the item type, and points at the
    // signature to write.
    for expected in [
        "`NotAReader` is not an OxideBatch item reader for `Invoice`",
        "this component cannot read `Invoice`",
        "async fn read(&mut self, context: ReadContext<'_>)",
        "the returned future must be `Send`",
    ] {
        assert!(
            stderr.contains(expected),
            "missing {expected:?} in diagnostic:\n{stderr}"
        );
    }
}

#[test]
fn a_non_send_body_is_rejected_at_the_offending_value() {
    let stderr = compile_fixture("non_send_body.rs");

    // The bound the trait declares is still enforced against a plain `async
    // fn` body, and the diagnostic names the value, the await, and the trait
    // bound that requires it.
    for expected in [
        "future cannot be sent between threads safely",
        "Rc<u32>",
        "await occurs here",
        "required by a bound in",
    ] {
        assert!(
            stderr.contains(expected),
            "missing {expected:?} in diagnostic:\n{stderr}"
        );
    }
}
