"""Compatibility wrapper for the production-safe soak gate package."""

from cra_no_action_soak.gate import SCHEMA, evaluate_no_action_soak, main, read_samples

__all__ = ["SCHEMA", "evaluate_no_action_soak", "main", "read_samples"]


if __name__ == "__main__":
    main()
