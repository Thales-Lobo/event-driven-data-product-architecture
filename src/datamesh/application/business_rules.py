"""Pluggable business-rule registry.

The paper stresses that a Data Product's *metadata* stores which rule version was
applied, not the formula itself. We honor that by keeping executable rules in a
versioned in-process registry keyed by ``rule_id``. New indicators are added by
registering a new callable -- the orchestrator stays generic.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

# A rule maps {base_data_id -> latest numeric value} to a single output value.
BusinessRule = Callable[[Mapping[str, float]], float]

_REGISTRY: dict[str, BusinessRule] = {}


def register_rule(rule_id: str) -> Callable[[BusinessRule], BusinessRule]:
    """Decorator registering a rule implementation under a stable id."""

    def _decorator(func: BusinessRule) -> BusinessRule:
        _REGISTRY[rule_id] = func
        return func

    return _decorator


def get_rule(rule_id: str) -> BusinessRule:
    """Resolve a rule by id, raising if the platform is misconfigured."""
    try:
        return _REGISTRY[rule_id]
    except KeyError as exc:  # pragma: no cover - configuration error
        raise KeyError(f"No business rule registered for id '{rule_id}'.") from exc


@register_rule("calc_gdp_weighted_avg")
def gdp_weighted_average(inputs: Mapping[str, float]) -> float:
    """Mock GDP: weighted average of industrial production and services revenue.

    GDP_v = (alpha * I) + (beta * S), with alpha=0.4, beta=0.6 (paper Eq. 4.1).
    Missing inputs raise a KeyError so incomplete recalculations fail loudly
    rather than silently producing a wrong indicator.
    """
    alpha, beta = 0.4, 0.6
    industrial = inputs["base_industrial_production"]
    services = inputs["base_services_revenue"]
    return round((alpha * industrial) + (beta * services), 4)