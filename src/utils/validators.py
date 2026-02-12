"""Input Validation Utilities"""

from typing import Any, Dict, Optional, TypeVar, List

T = TypeVar('T')


def safe_get_nested(
    data: Dict[str, Any],
    *keys: str,
    default: T = None
) -> T:
    """
    Safely access a nested dictionary.

    Examples::

        data = {"user": {"name": {"first": "John"}}}

        # Normal access
        first_name = safe_get_nested(data, "user", "name", "first")
        # -> "John"

        # Missing key
        middle_name = safe_get_nested(data, "user", "name", "middle", default="")
        # -> ""

        # Intermediate value is None
        data = {"user": None}
        first_name = safe_get_nested(data, "user", "name", "first", default="Unknown")
        # -> "Unknown"
    """
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return default
        if current is None:
            return default
    return current if current is not None else default


def sanitize_string(
    value: Any,
    default: str = "",
    max_length: Optional[int] = None,
    strip: bool = True
) -> str:
    """
    Sanitize a value into a clean string.

    Handles None, non-string types, leading/trailing whitespace, and truncation.

    Examples::

        sanitize_string(None)                       # -> ""
        sanitize_string(None, default="N/A")        # -> "N/A"
        sanitize_string("  hello  ")                # -> "hello"
        sanitize_string(12345)                      # -> "12345"
        sanitize_string("long text", max_length=4)  # -> "long"
    """
    if value is None:
        return default

    result = str(value)

    if strip:
        result = result.strip()

    if max_length and len(result) > max_length:
        result = result[:max_length]

    return result


def sanitize_email(email: Any) -> str:
    """
    Sanitize an email value (lowercase, strip whitespace).

    Example::

        sanitize_email("  John.Doe@Example.COM  ")  # -> "john.doe@example.com"
    """
    if email is None:
        return ""
    return str(email).strip().lower()


def validate_required_fields(
    data: Dict[str, Any],
    required_fields: List[str]
) -> List[str]:
    """Return a list of field names that are missing or empty in data."""
    missing = []
    for field in required_fields:
        value = data.get(field)
        if value is None or (isinstance(value, str) and value.strip() == ""):
            missing.append(field)
    return missing
