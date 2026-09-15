import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import urllib.request
import zipfile

PRODUCER_HEAD = "67a40eb0eb8722d7df35d23190be201c9cd6633d"
EXECUTION_COMMIT = "b5d1ca2206335714e66aa2ec5c7bd7d2911509ea"

# milestone|retained report|artifact inner file|run id|workflow|workflow file|job id|job name|artifact id|artifact name|digest|size
RECORDS = r'''m5|cancellation-campaign-postgres-15.json|cancellation-campaign.json|34933190141|M5 Cancellation|.github/workflows/m5-cancellation.yml|104265514633|postgres-15-cancellation-campaign|10382870182|cancellation-campaign-postgres-15|sha256:70c38b3b2c73ef860e48574d720cbbb0bc44621c8972a2a15634b7eea1a5df70|5745
m5|cancellation-campaign-postgres-18.json|cancellation-campaign.json|34933190141|M5 Cancellation|.github/workflows/m5-cancellation.yml|104265514426|postgres-18-cancellation-campaign|10381919727|cancellation-campaign-postgres-18|sha256:ca88f5c523c5999788d324ebac81cb962846eb2777bc28bd72867c7bc462aa81|5757
m5|conformance-campaign-postgres-15.json|conformance-campaign.json|34933190051|M5 Conformance|.github/workflows/m5-conformance.yml|104265514227|postgres-15-conformance-campaign|10381939698|conformance-campaign-postgres-15|sha256:5b20de219d6ba7c1b301772816d89c87172ec4897620ccf2be709f4d6ac56c6a|7137
m5|conformance-campaign-postgres-18.json|conformance-campaign.json|34933190051|M5 Conformance|.github/workflows/m5-conformance.yml|104265513992|postgres-18-conformance-campaign|10382266879|conformance-campaign-postgres-18|sha256:0c7503d834070aec64b76b794082c08331d94b7ad8f0ba75583af74909331da2|7137
m5|crash-restore-campaign-postgres-15.json|crash-restore-campaign.json|34933190085|M5 Crash and Restore|.github/workflows/m5-crash-restore.yml|104265514129|postgres-15-crash-restore-campaign|10382138981|crash-restore-campaign-postgres-15|sha256:6db853e0a6497d4aa1a8d3023d5a9c8a2d9ff61ffe28271dbe5d8f165e535816|6674
m5|crash-restore-campaign-postgres-18.json|crash-restore-campaign.json|34933190085|M5 Crash and Restore|.github/workflows/m5-crash-restore.yml|104265514344|postgres-18-crash-restore-campaign|10381844843|crash-restore-campaign-postgres-18|sha256:ad8765914245cee316a4f5fa40fc8ba5ca5d20dfe5848cda92e39b5df8320f91|6678
m5|performance-campaign-postgres-15.json|performance-campaign.json|34933190033|M5 Performance|.github/workflows/m5-performance.yml|104265514012|postgres-15-performance-campaign|10382870200|performance-campaign-postgres-15|sha256:b4dc764e5677e8e2717fede9f5620661c09e0a5bc14b2795801b05844773eb1c|6603
m5|performance-campaign-postgres-18.json|performance-campaign.json|34933190033|M5 Performance|.github/workflows/m5-performance.yml|104265514221|postgres-18-performance-campaign|10382975106|performance-campaign-postgres-18|sha256:58bcd5b3381f59e91720823bf598d511db316f95f6829d16d8314e434d229f87|6607
m5|resource-bounds-campaign-postgres-15.json|resource-bounds-campaign.json|34933190086|M5 Resource Bounds|.github/workflows/m5-resource-bounds.yml|104265514338|postgres-15-resource-bounds-campaign|10382124140|resource-bounds-campaign-postgres-15|sha256:9ed1b223048df05562a13356e3bbfbaff9f6043d75049d9c37e423f77e3e7d84|14186
m5|resource-bounds-campaign-postgres-18.json|resource-bounds-campaign.json|34933190086|M5 Resource Bounds|.github/workflows/m5-resource-bounds.yml|104265514146|postgres-18-resource-bounds-campaign|10382276725|resource-bounds-campaign-postgres-18|sha256:3e1abc6dcba1404deb8c94a90499483cdbd4794ecff3e296b6205b39540a719f|14185
m5|security-campaign-postgres-15.json|security-campaign.json|34933190034|M5 Security|.github/workflows/m5-security.yml|104265513985|postgres-15-security-campaign|10382223585|security-campaign-postgres-15|sha256:dafb428ebbd5726580f43932a6fbe5d4497179470fcb93e3d0c544292bf82ae7|7582
m5|security-campaign-postgres-18.json|security-campaign.json|34933190034|M5 Security|.github/workflows/m5-security.yml|104265514166|postgres-18-security-campaign|10381899763|security-campaign-postgres-18|sha256:f88be135abd7cd8215c173b9cc7c6147e1c3b622bdd388237355d75bd49341c5|7581
m5|soak-campaign-postgres-15.json|soak-campaign.json|34933190093|M5 Soak|.github/workflows/m5-soak.yml|104265514310|postgres-15-soak-campaign|10383100038|soak-campaign-postgres-15|sha256:478e8e40c927329489c38dbf8d5cc4ad9fbd5f2b8cd56199e8ac8e1fd49b8bcd|88506
m5|soak-campaign-postgres-18.json|soak-campaign.json|34933190093|M5 Soak|.github/workflows/m5-soak.yml|104265514127|postgres-18-soak-campaign|10382517073|soak-campaign-postgres-18|sha256:fcc15760ac8199de863dba90b2c42df89fa9029d992d27d47b8b605548866cbf|88264
m5|upgrade-campaign-postgres-15.json|upgrade-campaign.json|34933190097|M5 Upgrade|.github/workflows/m5-upgrade.yml|104265514141|postgres-15-upgrade-campaign|10382297745|upgrade-campaign-postgres-15|sha256:ea5c89a71e38680c3745ab3d4683fbc59a533e5648903ebf7bd89cdd73884b26|5756
m5|upgrade-campaign-postgres-18.json|upgrade-campaign.json|34933190097|M5 Upgrade|.github/workflows/m5-upgrade.yml|104265514603|postgres-18-upgrade-campaign|10382740542|upgrade-campaign-postgres-18|sha256:f1bc90d80573bd53afe8bb39c7198e49f8171b88a505d7b4f4420ceb2ff38c31|5757
m6|gate-b-campaign-postgres-15.json|gate-b-campaign.json|34933190084|M6 Gate B Transaction/Restart Equivalence|.github/workflows/m6-gate-b.yml|104265514246|postgres-15-gate-b-campaign|10382980057|gate-b-campaign-postgres-15|sha256:387305c5c1f87da15baa9dee159203ecfb93d4acd7e3cc3bca3fc241cb844507|2335
m6|gate-b-campaign-postgres-18.json|gate-b-campaign.json|34933190084|M6 Gate B Transaction/Restart Equivalence|.github/workflows/m6-gate-b.yml|104265514396|postgres-18-gate-b-campaign|10382701119|gate-b-campaign-postgres-18|sha256:8e633b32523d09594dba31c4d21d24f73fdef81cbcf2bd52d57b34e2e580f6b3|2335
m6|gate-h-campaign.json|gate-h-campaign.json|34933190080|M6 Gate H P-002 Performance|.github/workflows/m6-gate-h.yml|104265514097|gate-h-campaign|10382044322|gate-h-campaign|sha256:2d7ae188fe56dedce8378dc5999518dd7be49b49e6deb55bfaf8f5c417be87bc|3151
m6|m6-conformance-campaign-postgres-15.json|m6-conformance-campaign.json|34933190018|M6 Full Component Conformance|.github/workflows/m6-conformance.yml|104265514288|postgres-15-m6-conformance|10382965288|m6-conformance-campaign-postgres-15|sha256:ed75f9d7d774c991b94d60a40b0f29787bf3ff0026fe8d2d4288db7643f4ff7c|10430
m6|m6-conformance-campaign-postgres-18.json|m6-conformance-campaign.json|34933190018|M6 Full Component Conformance|.github/workflows/m6-conformance.yml|104265514189|postgres-18-m6-conformance|10382054456|m6-conformance-campaign-postgres-18|sha256:7ea6e297826f0f5f9eb063d37330b6e01e1a996256fa915651ae23fa998aac8c|10430'''

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
            "User-Agent": "oxide-batch-pr272-evidence-promotion",
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


manifest = []
for line in RECORDS.splitlines():
    fields = line.split("|")
    if len(fields) != 12:
        raise SystemExit(f"malformed record: {line}")
    milestone, report, inner, run_id, workflow, workflow_file, job_id, job_name, artifact_id, artifact_name, digest, size = fields
    manifest.append(
        {
            "milestone": milestone,
            "report": report,
            "inner": inner,
            "run_id": int(run_id),
            "workflow": workflow,
            "workflow_file": workflow_file,
            "job_id": int(job_id),
            "job_name": job_name,
            "artifact_id": int(artifact_id),
            "artifact_name": artifact_name,
            "digest": digest,
            "size": int(size),
        }
    )

provenance = {}
entries = {}
for milestone in ("m5", "m6"):
    path = Path(f"docs/engineering/campaigns/{milestone}/evidence-provenance.json")
    document = json.loads(path.read_text())
    provenance[milestone] = (path, document)
    for entry in document["evidence"]:
        key = (milestone, entry["report"])
        if key in entries:
            raise SystemExit(f"duplicate provenance entry: {key}")
        entries[key] = entry

expected_inventory = {(item["milestone"], item["report"]) for item in manifest}
if set(entries) != expected_inventory:
    raise SystemExit(
        f"manifest/provenance inventory mismatch missing={set(entries) - expected_inventory} extra={expected_inventory - set(entries)}"
    )

verified_at = (
    datetime.datetime.now(datetime.timezone.utc)
    .replace(microsecond=0)
    .isoformat()
    .replace("+00:00", "Z")
)

for item in manifest:
    entry = entries[(item["milestone"], item["report"])]

    run = api_json(f"/actions/runs/{item['run_id']}")
    assert run["id"] == item["run_id"]
    assert run["name"] == item["workflow"]
    assert run["path"] == item["workflow_file"]
    assert run["event"] == "pull_request"
    assert run["conclusion"] == "success"
    assert run["run_attempt"] == 1
    assert run["head_sha"] == PRODUCER_HEAD
    assert run["head_branch"] == "dependabot/cargo/cargo-minor-and-patch-32f95b6a42"

    job = api_json(f"/actions/jobs/{item['job_id']}")
    assert job["id"] == item["job_id"]
    assert job["run_id"] == item["run_id"]
    assert job["name"] == item["job_name"]
    assert job["status"] == "completed"
    assert job["conclusion"] == "success"

    artifact = api_json(f"/actions/artifacts/{item['artifact_id']}")
    assert artifact["id"] == item["artifact_id"]
    assert artifact["name"] == item["artifact_name"]
    assert artifact["size_in_bytes"] == item["size"]
    assert artifact["digest"] == item["digest"]
    assert not artifact["expired"]
    assert artifact["workflow_run"]["id"] == item["run_id"]
    assert artifact["workflow_run"]["head_sha"] == PRODUCER_HEAD

    archive = archive_bytes(item["artifact_id"])
    observed_digest = "sha256:" + hashlib.sha256(archive).hexdigest()
    assert observed_digest == item["digest"]

    with tempfile.TemporaryDirectory() as directory:
        archive_path = Path(directory) / "artifact.zip"
        archive_path.write_bytes(archive)
        with zipfile.ZipFile(archive_path) as zipped:
            names = [name for name in zipped.namelist() if not name.endswith("/")]
            assert names == [item["inner"]], names
            report_bytes = zipped.read(item["inner"])

    report = json.loads(report_bytes)
    env = report["environment"]
    assert env["source_commit"] == EXECUTION_COMMIT
    assert env["source_tree_clean"] is True
    assert env["matrix"] == entry["matrix_point"]
    assert str(report["postgresql_major_version"]) == entry["postgres_major_version"]
    assert report["passed"] is True
    assert report["violations"] == []

    destination = Path("docs/engineering/campaigns") / item["milestone"] / item["report"]
    destination.write_bytes(report_bytes)
    retained_blob = git_blob(report_bytes)

    producer = entry["producer"]
    producer["execution_commit"] = env["source_commit"]
    producer["execution_commit_note"] = (
        "Exact PR #272 synthetic merge execution tree recorded by the successful producer report; "
        "object-manifest identities are the authority for later local verification."
    )
    producer["branch_head_sha"] = PRODUCER_HEAD
    producer["branch_head_note"] = (
        "Exact PR #272 source candidate that triggered the successful producer run; recorded "
        "separately from the ephemeral execution tree."
    )
    producer["source_tree_clean"] = True
    producer["rustc"] = env["rustc"]
    producer["os"] = env["os"]
    producer["arch"] = env["arch"]

    entry["workflow_run"].update(
        {
            "workflow": item["workflow"],
            "workflow_file": item["workflow_file"],
            "id": item["run_id"],
            "attempt": 1,
            "event": "pull_request",
            "conclusion": "success",
        }
    )
    entry["producing_job"].update(
        {"name": item["job_name"], "id": item["job_id"], "conclusion": "success"}
    )
    entry["artifact"].update(
        {
            "name": item["artifact_name"],
            "id": item["artifact_id"],
            "digest": item["digest"],
            "size_bytes": item["size"],
        }
    )
    entry["retained_report_git_blob"] = retained_blob
    entry["remote_verification"] = {
        "verified": True,
        "verified_at": verified_at,
        "run_id": item["run_id"],
        "workflow_run_identity": True,
        "workflow_run_conclusion": True,
        "producing_job_identity": True,
        "producing_job_conclusion": True,
        "artifact_digest": True,
        "artifact_bytes_match_retained_report": True,
        "execution_commit_matches_report": True,
        "note": (
            "PR #272 successful Actions artifact independently fetched through the GitHub API; "
            "run/job identity and success, archive SHA-256, exact report bytes, canonical report "
            "identity, runtime axis when declared, and execution commit were verified before retention."
        ),
    }

for path, document in provenance.values():
    path.write_text(json.dumps(document, indent=2) + "\n")

print(f"promoted and re-verified {len(manifest)} fresh PR #272 artifacts")
