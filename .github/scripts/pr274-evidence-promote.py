import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import urllib.request
import zipfile

PRODUCER_HEAD = "4ab6cfd490bf5dae0eedabc610aabf6ce40c601e"
BASE_SHA = "67b3b6a043284af0afec1bb38259bb6a69575207"
EXECUTION_COMMIT = "1fc720d63ee0c16b5de2eb675ec349f56c8c14d8"
RUN_ID = 34963278048
WORKFLOW = "M6 Full Component Conformance"
WORKFLOW_FILE = ".github/workflows/m6-conformance.yml"

# report|inner|job id|job name|artifact id|artifact name|digest|size|matrix|postgres major
RECORDS = r'''m6-conformance-campaign-postgres-15.json|m6-conformance-campaign.json|104361814601|postgres-15-m6-conformance|10394301287|m6-conformance-campaign-postgres-15|sha256:acc33609c4684feb638527867b1bf68342672c31b5849710fcf2c410715156d7|10430|postgres-15|15
m6-conformance-campaign-postgres-18.json|m6-conformance-campaign.json|104361814350|postgres-18-m6-conformance|10393354822|m6-conformance-campaign-postgres-18|sha256:3a1ce698f4c771ea809a813ab6e4ac245b27daa474961a38b7dc94445c60dc16|10430|postgres-18|18'''

repo = os.environ["REPOSITORY"]
token = os.environ["GH_TOKEN"]
api_root = f"https://api.github.com/repos/{repo}"


def api_json(path: str):
    request = urllib.request.Request(
        api_root + path,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "oxide-batch-pr274-evidence-promotion",
        },
    )
    with urllib.request.urlopen(request) as response:
        return json.load(response)


def archive_bytes(artifact_id: int) -> bytes:
    with tempfile.NamedTemporaryFile() as temp:
        subprocess.run(
            [
                "curl",
                "-fsSL",
                "-H",
                f"Authorization: Bearer {token}",
                "-H",
                "Accept: application/vnd.github+json",
                "-H",
                "X-GitHub-Api-Version: 2022-11-28",
                f"{api_root}/actions/artifacts/{artifact_id}/zip",
                "-o",
                temp.name,
            ],
            check=True,
        )
        return Path(temp.name).read_bytes()


def git_blob(data: bytes) -> str:
    return hashlib.sha1(f"blob {len(data)}\0".encode() + data).hexdigest()


run = api_json(f"/actions/runs/{RUN_ID}")
assert run["id"] == RUN_ID
assert run["name"] == WORKFLOW
assert run["path"] == WORKFLOW_FILE
assert run["event"] == "pull_request"
assert run["status"] == "completed"
assert run["conclusion"] == "success"
assert run["run_attempt"] == 1
assert run["head_sha"] == PRODUCER_HEAD
assert run["head_branch"] == "audit-271-custom-leaf-state-outcomes"

execution = api_json(f"/commits/{EXECUTION_COMMIT}")
assert execution["sha"] == EXECUTION_COMMIT
assert [parent["sha"] for parent in execution["parents"]] == [BASE_SHA, PRODUCER_HEAD]

path = Path("docs/engineering/campaigns/m6/evidence-provenance.json")
document = json.loads(path.read_text())
entries = {entry["report"]: entry for entry in document["evidence"]}

verified_at = (
    datetime.datetime.now(datetime.timezone.utc)
    .replace(microsecond=0)
    .isoformat()
    .replace("+00:00", "Z")
)

for line in RECORDS.splitlines():
    fields = line.split("|")
    if len(fields) != 10:
        raise SystemExit(f"malformed record: {line}")
    report_name, inner, job_id, job_name, artifact_id, artifact_name, digest, size, matrix, pg_major = fields
    job_id = int(job_id)
    artifact_id = int(artifact_id)
    size = int(size)

    entry = entries[report_name]
    assert entry["matrix_point"] == matrix
    assert entry["postgres_major_version"] == pg_major

    job = api_json(f"/actions/jobs/{job_id}")
    assert job["id"] == job_id
    assert job["run_id"] == RUN_ID
    assert job["name"] == job_name
    assert job["status"] == "completed"
    assert job["conclusion"] == "success"

    artifact = api_json(f"/actions/artifacts/{artifact_id}")
    assert artifact["id"] == artifact_id
    assert artifact["name"] == artifact_name
    assert artifact["size_in_bytes"] == size
    assert artifact["digest"] == digest
    assert not artifact["expired"]
    assert artifact["workflow_run"]["id"] == RUN_ID
    assert artifact["workflow_run"]["head_sha"] == PRODUCER_HEAD

    archive = archive_bytes(artifact_id)
    assert "sha256:" + hashlib.sha256(archive).hexdigest() == digest

    with tempfile.TemporaryDirectory() as directory:
        archive_path = Path(directory) / "artifact.zip"
        archive_path.write_bytes(archive)
        with zipfile.ZipFile(archive_path) as zipped:
            names = [name for name in zipped.namelist() if not name.endswith("/")]
            assert names == [inner], names
            report_bytes = zipped.read(inner)

    report = json.loads(report_bytes)
    env = report["environment"]
    assert env["source_commit"] == EXECUTION_COMMIT
    assert env["source_tree_clean"] is True
    assert env["matrix"] == matrix
    assert str(report["postgresql_major_version"]) == pg_major
    assert report["passed"] is True
    assert report["violations"] == []

    destination = Path("docs/engineering/campaigns/m6") / report_name
    destination.write_bytes(report_bytes)
    retained_blob = git_blob(report_bytes)

    producer = entry["producer"]
    producer.update(
        {
            "execution_commit": EXECUTION_COMMIT,
            "execution_commit_note": (
                "Exact PR #274 synthetic merge execution tree recorded by the successful producer report; "
                "object-manifest identities are the authority for later local verification."
            ),
            "branch_head_sha": PRODUCER_HEAD,
            "branch_head_note": (
                "Exact PR #274 source candidate that triggered the successful producer run; recorded "
                "separately from the ephemeral execution tree."
            ),
            "source_tree_clean": True,
            "rustc": env["rustc"],
            "os": env["os"],
            "arch": env["arch"],
        }
    )
    entry["workflow_run"].update(
        {
            "workflow": WORKFLOW,
            "workflow_file": WORKFLOW_FILE,
            "id": RUN_ID,
            "attempt": 1,
            "event": "pull_request",
            "conclusion": "success",
        }
    )
    entry["producing_job"].update(
        {"name": job_name, "id": job_id, "conclusion": "success"}
    )
    entry["artifact"].update(
        {
            "name": artifact_name,
            "id": artifact_id,
            "digest": digest,
            "size_bytes": size,
        }
    )
    entry["retained_report_git_blob"] = retained_blob
    entry["remote_verification"] = {
        "verified": True,
        "verified_at": verified_at,
        "run_id": RUN_ID,
        "workflow_run_identity": True,
        "workflow_run_conclusion": True,
        "producing_job_identity": True,
        "producing_job_conclusion": True,
        "artifact_digest": True,
        "artifact_bytes_match_retained_report": True,
        "execution_commit_matches_report": True,
        "note": (
            "PR #274 successful Actions artifact independently fetched through the GitHub API; "
            "run/job identity and success, archive SHA-256, exact report bytes, canonical report "
            "identity, matrix axis, PostgreSQL major, and execution commit were verified before retention."
        ),
    }

path.write_text(json.dumps(document, indent=2) + "\n")
print("promoted and re-verified 2 fresh PR #274 M6 conformance artifacts")
