import datetime as dt
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

MODULE_PATH = Path(__file__).with_name("validate_supply_chain_exceptions.py")
spec = importlib.util.spec_from_file_location("validate_supply_chain_exceptions", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(module)

TODAY = dt.date(2026, 9, 4)


def base_deny():
    return {
        "advisories": {"ignore": []},
        "licenses": {"allow": ["Apache-2.0", "MIT"], "exceptions": []},
        "bans": {
            "allow": [],
            "deny": [],
            "skip": [],
            "skip-tree": [],
            "build": {"bypass": []},
        },
        "sources": {
            "allow-registry": sorted(module.BASELINE_REGISTRIES),
            "allow-git": [],
            "allow-org": {},
        },
    }


class SupplyChainExceptionPolicyTests(unittest.TestCase):
    def test_current_empty_exception_state_is_valid(self):
        self.assertEqual(
            module.validate(base_deny(), {"schema_version": 1, "exceptions": []}, TODAY),
            [],
        )

    def test_unregistered_advisory_ignore_is_rejected(self):
        deny = base_deny()
        deny["advisories"]["ignore"] = ["RUSTSEC-2099-0001"]
        violations = module.validate(deny, {"schema_version": 1, "exceptions": []}, TODAY)
        self.assertTrue(any("has no owner/rationale/expiry" in v for v in violations))

    def test_missing_owner_rationale_and_expiry_are_rejected(self):
        deny = base_deny()
        deny["advisories"]["ignore"] = ["RUSTSEC-2099-0001"]
        registry = {
            "schema_version": 1,
            "exceptions": [{"kind": "advisory", "target": "RUSTSEC-2099-0001"}],
        }
        violations = module.validate(deny, registry, TODAY)
        self.assertTrue(any("owner" in v for v in violations))
        self.assertTrue(any("rationale" in v for v in violations))
        self.assertTrue(any("expires" in v for v in violations))

    def test_validator_cli_rejects_throwaway_exception_without_required_metadata(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            deny_path = root / "deny.toml"
            registry_path = root / "supply-chain-exceptions.json"
            deny_path.write_text(
                '[advisories]\nignore = ["RUSTSEC-2099-0001"]\n',
                encoding="utf-8",
            )
            registry_path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "exceptions": [
                            {"kind": "advisory", "target": "RUSTSEC-2099-0001"}
                        ],
                    }
                ),
                encoding="utf-8",
            )

            result = subprocess.run(
                [
                    sys.executable,
                    str(MODULE_PATH),
                    "--deny",
                    str(deny_path),
                    "--registry",
                    str(registry_path),
                    "--today",
                    TODAY.isoformat(),
                ],
                check=False,
                capture_output=True,
                text=True,
            )

        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("missing non-empty owner", result.stderr)
        self.assertIn("missing non-empty rationale", result.stderr)
        self.assertIn("missing non-empty expires", result.stderr)

    def test_expired_exception_is_rejected(self):
        deny = base_deny()
        deny["advisories"]["ignore"] = ["RUSTSEC-2099-0001"]
        registry = {
            "schema_version": 1,
            "exceptions": [{
                "kind": "advisory",
                "target": "RUSTSEC-2099-0001",
                "owner": "maintainer",
                "rationale": "temporary upstream remediation window",
                "expires": "2026-09-03",
            }],
        }
        violations = module.validate(deny, registry, TODAY)
        self.assertTrue(any("expired on 2026-09-03" in v for v in violations))

    def test_future_registered_advisory_exception_is_valid(self):
        deny = base_deny()
        deny["advisories"]["ignore"] = ["RUSTSEC-2099-0001"]
        registry = {
            "schema_version": 1,
            "exceptions": [{
                "kind": "advisory",
                "target": "RUSTSEC-2099-0001",
                "owner": "maintainer",
                "rationale": "temporary upstream remediation window",
                "expires": "2026-09-30",
            }],
        }
        self.assertEqual(module.validate(deny, registry, TODAY), [])

    def test_unregistered_license_exception_is_rejected(self):
        deny = base_deny()
        deny["licenses"]["exceptions"] = [
            {"crate": "example", "allow": ["Example-1.0"]}
        ]
        violations = module.validate(deny, {"schema_version": 1, "exceptions": []}, TODAY)
        self.assertTrue(any("license:" in v for v in violations))

    def test_registered_license_exception_is_valid(self):
        deny = base_deny()
        exception = {"crate": "example", "allow": ["Example-1.0"]}
        deny["licenses"]["exceptions"] = [exception]
        target = module._target(exception)
        registry = {
            "schema_version": 1,
            "exceptions": [{
                "kind": "license",
                "target": target,
                "owner": "maintainer",
                "rationale": "temporary upstream relicensing window",
                "expires": "2026-09-30",
            }],
        }
        self.assertEqual(module.validate(deny, registry, TODAY), [])

    def test_restrictive_license_baseline_change_is_not_treated_as_temporary_exception(self):
        deny = base_deny()
        deny["licenses"]["allow"].append("BSD-3-Clause")
        self.assertEqual(
            module.validate(deny, {"schema_version": 1, "exceptions": []}, TODAY),
            [],
        )

    def test_unregistered_ban_skip_is_rejected(self):
        deny = base_deny()
        deny["bans"]["skip"] = [{"name": "example", "version": "1"}]
        violations = module.validate(deny, {"schema_version": 1, "exceptions": []}, TODAY)
        self.assertTrue(any("ban:skip:" in v for v in violations))

    def test_unregistered_ban_skip_tree_is_rejected(self):
        deny = base_deny()
        deny["bans"]["skip-tree"] = [{"name": "example", "version": "1"}]
        violations = module.validate(deny, {"schema_version": 1, "exceptions": []}, TODAY)
        self.assertTrue(any("ban:skip-tree:" in v for v in violations))

    def test_unregistered_build_bypass_is_rejected(self):
        deny = base_deny()
        deny["bans"]["build"]["bypass"] = [
            {"name": "example", "build-script": "0" * 64}
        ]
        violations = module.validate(deny, {"schema_version": 1, "exceptions": []}, TODAY)
        self.assertTrue(any("ban:build-bypass:" in v for v in violations))

    def test_restrictive_ban_deny_is_not_treated_as_temporary_exception(self):
        deny = base_deny()
        deny["bans"]["deny"] = ["openssl"]
        self.assertEqual(
            module.validate(deny, {"schema_version": 1, "exceptions": []}, TODAY),
            [],
        )

    def test_unregistered_git_source_allow_is_rejected(self):
        deny = base_deny()
        deny["sources"]["allow-git"] = ["https://example.invalid/repo"]
        violations = module.validate(deny, {"schema_version": 1, "exceptions": []}, TODAY)
        self.assertTrue(any("source:git:" in v for v in violations))

    def test_unregistered_alternate_registry_is_rejected(self):
        deny = base_deny()
        deny["sources"]["allow-registry"].append("https://example.invalid/index")
        violations = module.validate(deny, {"schema_version": 1, "exceptions": []}, TODAY)
        self.assertTrue(any("source:registry:" in v for v in violations))

    def test_unregistered_allowed_source_org_is_rejected(self):
        deny = base_deny()
        deny["sources"]["allow-org"] = {"github": ["example-org"]}
        violations = module.validate(deny, {"schema_version": 1, "exceptions": []}, TODAY)
        self.assertTrue(any("source:org:github:example-org" in v for v in violations))

    def test_registered_source_org_is_valid(self):
        deny = base_deny()
        deny["sources"]["allow-org"] = {"github": ["example-org"]}
        registry = {
            "schema_version": 1,
            "exceptions": [{
                "kind": "source",
                "target": "org:github:example-org",
                "owner": "maintainer",
                "rationale": "temporary upstream source exception",
                "expires": "2026-09-30",
            }],
        }
        self.assertEqual(module.validate(deny, registry, TODAY), [])

    def test_stale_registry_entry_without_active_exception_is_rejected(self):
        registry = {
            "schema_version": 1,
            "exceptions": [{
                "kind": "advisory",
                "target": "RUSTSEC-2099-0001",
                "owner": "maintainer",
                "rationale": "temporary",
                "expires": "2026-09-30",
            }],
        }
        violations = module.validate(base_deny(), registry, TODAY)
        self.assertTrue(any("does not correspond" in v for v in violations))


if __name__ == "__main__":
    unittest.main()
