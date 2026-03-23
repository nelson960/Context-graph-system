from __future__ import annotations


class ContextGraphError(Exception):
    """Base class for app-specific errors."""


class ConfigurationError(ContextGraphError):
    """Raised when runtime configuration is missing or invalid."""


class PlannerError(ContextGraphError):
    """Raised when the planner fails to classify, plan, or compose."""


class OutOfDomainError(ContextGraphError):
    """Raised when the user prompt is outside the supported business domain."""


class EntityResolutionError(ContextGraphError):
    """Raised when entities cannot be resolved from the dataset."""


class AmbiguousEntityError(EntityResolutionError):
    """Raised when a user reference matches multiple entities with similar confidence."""


class SqlValidationError(ContextGraphError):
    """Raised when generated SQL violates the query contract."""


class QueryExecutionError(ContextGraphError):
    """Raised when validated SQL fails during execution."""
