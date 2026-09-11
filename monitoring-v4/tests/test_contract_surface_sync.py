from __future__ import annotations

import json
import unittest
from pathlib import Path

from stream_contracts.monitoring_v4.observation import DOMAIN_ORDER, DOMAINS
from stream_monitoring_v4.adapters.factory import SHADOW_DOMAINS
from stream_monitoring_v4.domains.source_policy import DEFAULT_POLICIES
from stream_monitoring_v4.incidents.policy import DEFAULT_INCIDENT_POLICIES
from stream_monitoring_v4.runtime.safe_inputs.generation import (
    SAFE_GENERATION_SCHEMA,
    projected_file_names,
)
from stream_monitoring_v4.runtime.source_revision import MAX_SOURCE_REVISION_LENGTH


ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = ROOT / "src" / "stream_contracts" / "monitoring_v4" / "schema"


class ContractSurfaceSyncTests(unittest.TestCase):
    def test_domain_contract_policy_factory_and_json_schema_are_exactly_aligned(self) -> None:
        observation_schema = json.loads(
            (SCHEMAS / "observation.v1.json").read_text(encoding="utf-8")
        )
        schema_domains = observation_schema["properties"]["domain"]["enum"]

        self.assertEqual(tuple(schema_domains), DOMAIN_ORDER)
        self.assertEqual(SHADOW_DOMAINS, DOMAIN_ORDER)
        self.assertEqual(set(DOMAINS), set(DEFAULT_POLICIES))
        self.assertEqual(set(DOMAINS), set(DEFAULT_INCIDENT_POLICIES))

    def test_safe_input_schema_version_and_exact_file_count_are_aligned(self) -> None:
        generation_schema = json.loads(
            (SCHEMAS / "safe_input_generation.v2.json").read_text(encoding="utf-8")
        )
        projection_schema = json.loads(
            (SCHEMAS / "safe_input_projection.v4.json").read_text(encoding="utf-8")
        )
        count = len(projected_file_names())
        expected_names = set(projected_file_names())
        generation_files = generation_schema["properties"]["files"]
        projection_properties = projection_schema["properties"]

        self.assertEqual(SAFE_GENERATION_SCHEMA, generation_schema["$id"])
        self.assertEqual(count, 21)
        self.assertEqual(generation_files["minProperties"], count)
        self.assertEqual(generation_files["maxProperties"], count)
        self.assertFalse(generation_files["additionalProperties"])
        self.assertEqual(set(generation_files["required"]), expected_names)
        self.assertEqual(set(generation_files["properties"]), expected_names)
        standard_names = expected_names - {
            "runtime_lifecycle_events.json",
            "deployed-revision.env",
        }
        self.assertEqual(
            {
                name
                for name, schema in generation_files["properties"].items()
                if schema["$ref"] == "#/$defs/standardMember"
            },
            standard_names,
        )
        self.assertEqual(
            generation_files["properties"]["runtime_lifecycle_events.json"]["$ref"],
            "#/$defs/member",
        )
        self.assertEqual(
            generation_files["properties"]["deployed-revision.env"]["$ref"],
            "#/$defs/revisionMember",
        )
        self.assertEqual(
            generation_schema["$defs"]["standardMember"]["properties"]["size_bytes"]["maximum"],
            4 * 1024 * 1024,
        )
        self.assertEqual(
            generation_schema["$defs"]["member"]["properties"]["size_bytes"]["maximum"],
            20 * 1024 * 1024,
        )
        self.assertEqual(
            generation_schema["$defs"]["revisionMember"]["properties"]["size_bytes"]["maximum"],
            4096,
        )
        self.assertEqual(
            generation_schema["properties"]["source_revision"]["maxLength"],
            MAX_SOURCE_REVISION_LENGTH,
        )
        self.assertEqual(
            projection_properties["source_revision"]["maxLength"],
            MAX_SOURCE_REVISION_LENGTH,
        )
        for field in ("projected", "unchanged"):
            self.assertEqual(projection_properties[field]["maxItems"], count)
            self.assertTrue(projection_properties[field]["uniqueItems"])
            self.assertEqual(
                set(projection_schema["$defs"]["memberName"]["enum"]),
                expected_names,
            )
        self.assertEqual(
            set(projection_properties["rejected"]["propertyNames"]["enum"]),
            expected_names | {".projection-generation.json"},
        )


if __name__ == "__main__":
    unittest.main()
