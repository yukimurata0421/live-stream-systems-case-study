"""Pure domain reduction for current-state decisions."""

from .reducer import reduce_domain
from .source_policy import DEFAULT_POLICIES, DomainPolicy, SourceRule

__all__ = ["DEFAULT_POLICIES", "DomainPolicy", "SourceRule", "reduce_domain"]
