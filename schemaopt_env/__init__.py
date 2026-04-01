"""Schema optimization OpenEnv environment package."""

from .models import SchemaOptAction, SchemaOptObservation, SchemaOptState

try:
    from .client import SchemaOptEnv
except Exception:  # pragma: no cover - optional in dependency-light local runs
    SchemaOptEnv = None

__all__ = ["SchemaOptAction", "SchemaOptObservation", "SchemaOptState", "SchemaOptEnv"]
