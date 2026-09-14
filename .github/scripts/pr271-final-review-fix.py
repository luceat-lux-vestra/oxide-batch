from pathlib import Path
import subprocess

BASE = "552a4575212f0ab0bc43072ddb7152e606e7bfe1"
subprocess.run(["git", "checkout", BASE, "--", "Cargo.lock"], check=True)

review = Path("crates/oxide-batch/tests/facade_review.rs")
text = review.read_text()
old = '    ("completion", 11),\n    ("diagnostics", 9),\n'
new = '    ("completion", 11),\n    ("custom_leaf", 4),\n    ("diagnostics", 9),\n'
if new not in text:
    if text.count(old) != 1:
        raise SystemExit(f"facade custom_leaf group anchor count: {text.count(old)}")
    text = text.replace(old, new, 1)
old = '''    // MAX_SELECTOR_PATH_BYTES, MAX_SELECTOR_PATH_SEGMENTS,
    // MissingParameterPolicy, NestedJobNode, NestedJobParameterMapping,
    // NestedJobParameterSource, ParameterCoercion, and SelectorPath.
    ("oxide_batch_plan", 39),
'''
new = '''    // MAX_SELECTOR_PATH_BYTES, MAX_SELECTOR_PATH_SEGMENTS,
    // MissingParameterPolicy, NestedJobNode, NestedJobParameterMapping,
    // NestedJobParameterSource, ParameterCoercion, and SelectorPath. #266 adds
    // CustomLeafKind and CustomLeafNode.
    ("oxide_batch_plan", 41),
'''
if new not in text:
    if text.count(old) != 1:
        raise SystemExit(f"facade plan-count anchor count: {text.count(old)}")
    text = text.replace(old, new, 1)
review.write_text(text)

snapshot = Path("crates/oxide-batch/tests/fixtures/facade/public-api.txt")
lines = [line for line in snapshot.read_text().splitlines() if line]
additions = {
    "oxide_batch::CustomLeafContext",
    "oxide_batch::CustomLeafHandler",
    "oxide_batch::CustomLeafKind",
    "oxide_batch::CustomLeafNode",
    "oxide_batch::CustomLeafRegistration",
    "oxide_batch::CustomLeafResult",
}
before = set(lines)
if additions & before and not additions <= before:
    raise SystemExit("partial custom-leaf API snapshot already present")
merged = sorted(before | additions)
expected_growth = 0 if additions <= before else 6
if len(merged) != len(before) + expected_growth:
    raise SystemExit("unexpected public API snapshot cardinality")
snapshot.write_text("\n".join(merged) + "\n")
