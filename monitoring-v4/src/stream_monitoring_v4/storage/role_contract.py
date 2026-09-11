from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class RoleTablePrivileges:
    select: frozenset[str] = frozenset()
    insert: frozenset[str] = frozenset()
    update: frozenset[str] = frozenset()
    delete: frozenset[str] = frozenset()
    select_all: bool = False

    def tables_for(self, privilege: str, *, all_tables: frozenset[str]) -> frozenset[str]:
        if privilege == "SELECT" and self.select_all:
            return all_tables
        return {
            "SELECT": self.select,
            "INSERT": self.insert,
            "UPDATE": self.update,
            "DELETE": self.delete,
        }[privilege]


CORE_WRITE_TABLES = frozenset(
    {
        "observations",
        "rejections",
        "domain_current",
        "current_snapshots",
        "incident_candidates",
        "incident_episodes",
        "incident_transitions",
        "incident_processed_currents",
        "notification_intents",
        "notification_intent_delivery",
        "leases",
        "lease_fences",
        "component_health",
        "sli_projections",
        "sli_projection_current",
        "shadow_cycles",
        "public_artifact_publications",
    }
)
NOTIFIER_WRITE_TABLES = frozenset(
    {
        "delivery_attempts",
        "delivery_attempt_states",
        "delivery_results",
        "leases",
        "lease_fences",
    }
)

ROLE_TABLE_PRIVILEGES = {
    "v4_core_rw": RoleTablePrivileges(
        select=CORE_WRITE_TABLES
        | frozenset({"schema_migrations", "notification_delivery_epochs"}),
        insert=CORE_WRITE_TABLES,
        update=CORE_WRITE_TABLES,
        delete=CORE_WRITE_TABLES,
    ),
    "v4_exporter_ro": RoleTablePrivileges(
        select=frozenset(
            {
                "schema_migrations",
                "observations",
                "rejections",
                "domain_current",
                "incident_transitions",
                "notification_intents",
                "notification_intent_delivery",
                "notification_delivery_epochs",
                "delivery_attempts",
                "delivery_attempt_states",
                "delivery_results",
                "sli_projections",
                "sli_projection_current",
                "shadow_cycles",
                "public_artifact_publications",
            }
        )
    ),
    "v4_reporter_ro": RoleTablePrivileges(
        select=frozenset({"schema_migrations", "shadow_cycles"})
    ),
    "v4_backup_ro": RoleTablePrivileges(select_all=True),
    "v4_notifier_rw": RoleTablePrivileges(
        select=frozenset(
            {
                "notification_intents",
                "notification_intent_delivery",
                "notification_delivery_epochs",
                "delivery_attempts",
                "delivery_attempt_states",
                "delivery_results",
                "leases",
                "lease_fences",
            }
        ),
        insert=NOTIFIER_WRITE_TABLES,
        update=NOTIFIER_WRITE_TABLES,
        delete=NOTIFIER_WRITE_TABLES,
    ),
    "v4_maintenance_rw": RoleTablePrivileges(
        select=frozenset(
            {
                "schema_migrations",
                "shadow_cycles",
                "observations",
                "rejections",
                "sli_projections",
                "sli_projection_current",
                "current_snapshots",
                "domain_current",
                "incident_candidates",
                "incident_transitions",
                "incident_processed_currents",
                "public_artifact_publications",
            }
        ),
        delete=frozenset(
            {
                "shadow_cycles",
                "observations",
                "rejections",
                "sli_projections",
                "current_snapshots",
                "incident_processed_currents",
                "public_artifact_publications",
            }
        ),
    ),
}


def table_grant_statements() -> tuple[str, ...]:
    statements: list[str] = []
    for role, contract in ROLE_TABLE_PRIVILEGES.items():
        if contract.select_all:
            statements.append(f"GRANT SELECT ON ALL TABLES IN SCHEMA public TO {role}")
        else:
            for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                tables = sorted(contract.tables_for(privilege, all_tables=frozenset()))
                if tables:
                    statements.append(
                        f"GRANT {privilege} ON {', '.join(tables)} TO {role}"
                    )
    return tuple(statements)
