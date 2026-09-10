"""Compatibility wrapper for the production-safe soak sample package."""

from cra_no_action_soak.sample import append_sample, compose_no_action_sample, main

__all__ = ["append_sample", "compose_no_action_sample", "main"]


if __name__ == "__main__":
    main()
