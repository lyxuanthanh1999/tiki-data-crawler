"""Shared data models for the Selenium Worker Hybrid crawler."""

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class FetchResult:
    """Normalized result returned by one product-detail request."""

    status: str
    product: Optional[dict[str, Any]] = None
    reason: str = ""
    retry_after: Optional[int] = None
