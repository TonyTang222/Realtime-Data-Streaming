"""
Unit Tests for Validators
"""

import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.utils.validators import (
    safe_get_nested,
    sanitize_string,
    sanitize_email,
    validate_required_fields,
)


class TestSafeGetNested:
    """Tests for safe_get_nested."""

    def test_get_existing_value(self):
        """Test retrieving an existing value."""
        data = {"user": {"name": {"first": "John"}}}
        result = safe_get_nested(data, "user", "name", "first")
        assert result == "John"

    def test_get_missing_key_returns_default(self):
        """Test missing key returns the default."""
        data = {"user": {}}
        result = safe_get_nested(data, "user", "name", "first", default="Unknown")
        assert result == "Unknown"

    def test_get_from_none_returns_default(self):
        """Test intermediate None returns the default."""
        data = {"user": None}
        result = safe_get_nested(data, "user", "name", default="default")
        assert result == "default"

    def test_get_with_non_dict_returns_default(self):
        """Test non-dict intermediate returns the default."""
        data = {"user": "not a dict"}
        result = safe_get_nested(data, "user", "name", default="default")
        assert result == "default"

    def test_get_single_key(self):
        """Test single-level access."""
        data = {"name": "John"}
        result = safe_get_nested(data, "name")
        assert result == "John"

    def test_get_deep_nested(self):
        """Test deeply nested access."""
        data = {"a": {"b": {"c": {"d": {"e": "deep"}}}}}
        result = safe_get_nested(data, "a", "b", "c", "d", "e")
        assert result == "deep"


class TestSanitizeString:
    """Tests for sanitize_string."""

    def test_normal_string(self):
        """Test normal string passes through."""
        result = sanitize_string("Hello World")
        assert result == "Hello World"

    def test_none_returns_default(self):
        """Test None returns the default."""
        result = sanitize_string(None, default="N/A")
        assert result == "N/A"

    def test_strips_whitespace(self):
        """Test whitespace stripping."""
        result = sanitize_string("  trimmed  ")
        assert result == "trimmed"

    def test_truncates_long_string(self):
        """Test truncation of long strings."""
        long_string = "a" * 1000
        result = sanitize_string(long_string, max_length=100)
        assert len(result) == 100

    def test_converts_number_to_string(self):
        """Test number-to-string conversion."""
        result = sanitize_string(12345)
        assert result == "12345"

    def test_no_strip_option(self):
        """Test strip=False preserves whitespace."""
        result = sanitize_string("  spaces  ", strip=False)
        assert result == "  spaces  "


class TestSanitizeEmail:
    """Tests for sanitize_email."""

    def test_lowercase_email(self):
        """Test lowercasing."""
        result = sanitize_email("John.Doe@EXAMPLE.COM")
        assert result == "john.doe@example.com"

    def test_strips_whitespace(self):
        """Test whitespace stripping."""
        result = sanitize_email("  john@example.com  ")
        assert result == "john@example.com"

    def test_none_returns_empty(self):
        """Test None returns empty string."""
        result = sanitize_email(None)
        assert result == ""


class TestValidateRequiredFields:
    """Tests for validate_required_fields."""

    def test_all_fields_present(self):
        """Test all fields present."""
        data = {"name": "John", "email": "john@example.com"}
        missing = validate_required_fields(data, ["name", "email"])
        assert missing == []

    def test_missing_field(self):
        """Test missing fields are reported."""
        data = {"name": "John"}
        missing = validate_required_fields(data, ["name", "email", "phone"])
        assert "email" in missing
        assert "phone" in missing

    def test_empty_string_is_missing(self):
        """Test empty string treated as missing."""
        data = {"name": "John", "email": ""}
        missing = validate_required_fields(data, ["name", "email"])
        assert "email" in missing

    def test_whitespace_only_is_missing(self):
        """Test whitespace-only treated as missing."""
        data = {"name": "John", "email": "   "}
        missing = validate_required_fields(data, ["name", "email"])
        assert "email" in missing


