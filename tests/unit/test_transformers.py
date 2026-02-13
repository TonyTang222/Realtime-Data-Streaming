"""
Unit Tests for User Transformer
"""

import uuid

import pytest

from src.exceptions.custom_exceptions import TransformationError
from src.transformers.user_transformer import UserTransformer


class TestUserTransformer:
    """Tests for UserTransformer."""

    @pytest.fixture
    def transformer(self):
        """Create a transformer instance."""
        return UserTransformer()

    # ============================================================
    # Happy Path Tests
    # ============================================================

    def test_transform_valid_data(self, transformer, sample_api_response):
        """Test transformation of valid data."""
        raw_data = sample_api_response["results"][0]
        result = transformer.transform(raw_data)

        # Verify required fields exist
        assert "id" in result
        assert "first_name" in result
        assert "last_name" in result
        assert "email" in result

        # Verify correct values
        assert result["first_name"] == "John"
        assert result["last_name"] == "Doe"
        assert result["email"] == "john.doe@example.com"
        assert result["gender"] == "male"
        assert result["username"] == "johndoe"

    def test_transform_generates_valid_uuid(self, transformer, sample_api_response):
        """Test that generated UUID is valid."""
        raw_data = sample_api_response["results"][0]
        result = transformer.transform(raw_data)

        # Should parse as a valid UUID
        parsed_uuid = uuid.UUID(result["id"])
        assert str(parsed_uuid) == result["id"]

    def test_transform_generates_unique_ids(self, transformer, sample_api_response):
        """Test that each transform generates a unique ID."""
        raw_data = sample_api_response["results"][0]

        ids = set()
        for _ in range(100):
            result = transformer.transform(raw_data)
            ids.add(result["id"])

        # 100 runs should produce 100 unique IDs
        assert len(ids) == 100

    def test_transform_builds_correct_address(self, transformer, sample_api_response):
        """Test correct address formatting."""
        raw_data = sample_api_response["results"][0]
        result = transformer.transform(raw_data)

        address = result["address"]
        assert "123" in address  # Street number
        assert "Main Street" in address  # Street name
        assert "New York" in address  # City
        assert "10001" in address  # Postcode
        assert "NY" in address  # State
        assert "USA" in address  # Country

    def test_transform_extracts_dob(self, transformer, sample_api_response):
        """Test DOB field extraction."""
        raw_data = sample_api_response["results"][0]
        result = transformer.transform(raw_data)

        assert result["dob"] == "1990-01-15T10:30:00.000Z"

    def test_transform_extracts_registered_date(self, transformer, sample_api_response):
        """Test registered date extraction."""
        raw_data = sample_api_response["results"][0]
        result = transformer.transform(raw_data)

        assert result["registered_date"] == "2020-05-20T08:00:00.000Z"

    def test_transform_email_lowercase(self, transformer, sample_api_response):
        """Test email is lowercased."""
        raw_data = sample_api_response["results"][0]
        raw_data["email"] = "John.Doe@EXAMPLE.COM"

        result = transformer.transform(raw_data)

        assert result["email"] == "john.doe@example.com"

    # ============================================================
    # Edge Case Tests
    # ============================================================

    def test_transform_numeric_postcode(self, transformer, sample_api_response):
        """Test numeric postcode is converted to string."""
        raw_data = sample_api_response["results"][0]
        raw_data["location"]["postcode"] = 12345  # Numeric

        result = transformer.transform(raw_data)

        assert "12345" in result["address"]

    def test_transform_with_special_characters(self, transformer, sample_api_response):
        """Test special characters are handled correctly."""
        raw_data = sample_api_response["results"][0]
        raw_data["name"]["first"] = "Jean-Pierre"
        raw_data["name"]["last"] = "O'Brien"
        raw_data["location"]["street"]["name"] = "Rue de l'Arc"

        result = transformer.transform(raw_data)

        assert result["first_name"] == "Jean-Pierre"
        assert result["last_name"] == "O'Brien"
        assert "Rue de l'Arc" in result["address"]

    def test_transform_with_whitespace(self, transformer, sample_api_response):
        """Test whitespace is trimmed."""
        raw_data = sample_api_response["results"][0]
        raw_data["name"]["first"] = "  John  "
        raw_data["email"] = "  john@example.com  "

        result = transformer.transform(raw_data)

        assert result["first_name"] == "John"  # Trimmed
        assert result["email"] == "john@example.com"  # Trimmed

    def test_transform_empty_phone(self, transformer, sample_api_response):
        """Test empty phone does not cause an error."""
        raw_data = sample_api_response["results"][0]
        raw_data["phone"] = ""

        result = transformer.transform(raw_data)

        assert result["phone"] == ""

    # ============================================================
    # Error Case Tests
    # ============================================================

    def test_transform_missing_name_uses_default(self, transformer):
        """Test missing name falls back to default value."""
        data_without_name = {
            "gender": "male",
            # Missing name - will use default "Unknown"
            "location": {
                "street": {"number": 1, "name": "St"},
                "city": "C",
                "state": "S",
                "country": "C",
                "postcode": "1",
            },
            "email": "test@test.com",
            "login": {"username": "test"},
            "dob": {"date": "2000-01-01"},
            "registered": {"date": "2020-01-01"},
            "phone": "123",
            "picture": {"medium": "url"},
        }

        # Transformer uses default "Unknown" instead of raising
        result = transformer.transform(data_without_name)
        assert result["first_name"] == "Unknown"
        assert result["last_name"] == "Unknown"

    def test_transform_none_data_raises_error(self, transformer):
        """Test None input raises an error."""
        with pytest.raises((TransformationError, TypeError, AttributeError)):
            transformer.transform(None)

    def test_transform_empty_dict_raises_error(self, transformer):
        """Test empty dict raises an error."""
        with pytest.raises(TransformationError):
            transformer.transform({})
