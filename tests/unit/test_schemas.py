"""
Unit Tests for Data Schemas
"""

import pytest
import json
import uuid
import sys
from pathlib import Path
from datetime import datetime
from pydantic import ValidationError

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.config.schemas import (
    # Models
    StreetInfo,
    LocationInfo,
    NameInfo,
    LoginInfo,
    DOBInfo,
    RegisteredInfo,
    PictureInfo,
    UserAPIResponse,
    TransformedUser,
    DLQMessage,
    # Validation functions
    validate_api_response,
    validate_transformed_data,
    # Spark schema
    SPARK_USER_SCHEMA,
)


class TestStreetInfo:
    """Tests for StreetInfo model."""

    def test_valid_street_info(self):
        """Test valid street info."""
        street = StreetInfo(number=123, name="Main Street")
        assert street.number == 123
        assert street.name == "Main Street"

    def test_invalid_number_type(self):
        """Test invalid street number type."""
        with pytest.raises(ValidationError):
            StreetInfo(number="not a number", name="Main Street")


class TestLocationInfo:
    """Tests for LocationInfo model."""

    @pytest.fixture
    def valid_street(self):
        return {"number": 123, "name": "Main Street"}

    def test_valid_location(self, valid_street):
        """Test valid location."""
        location = LocationInfo(
            street=valid_street,
            city="New York",
            state="NY",
            country="USA",
            postcode="10001"
        )
        assert location.city == "New York"
        assert location.postcode == "10001"

    def test_numeric_postcode_converted_to_string(self, valid_street):
        """Test numeric postcode is converted to string."""
        location = LocationInfo(
            street=valid_street,
            city="Berlin",
            state="Berlin",
            country="Germany",
            postcode=10115  # Numeric
        )
        assert location.postcode == "10115"
        assert isinstance(location.postcode, str)

    def test_none_postcode_converted_to_empty_string(self, valid_street):
        """Test None postcode is converted to empty string."""
        location = LocationInfo(
            street=valid_street,
            city="Tokyo",
            state="Tokyo",
            country="Japan",
            postcode=None
        )
        assert location.postcode == ""


class TestUserAPIResponse:
    """Tests for UserAPIResponse model."""

    @pytest.fixture
    def valid_user_data(self, sample_api_response):
        """Get user data from sample_api_response."""
        return sample_api_response['results'][0]

    def test_valid_user_response(self, valid_user_data):
        """Test valid user response."""
        user = UserAPIResponse.parse_obj(valid_user_data)
        assert user.name.first == "John"
        assert user.name.last == "Doe"
        assert user.email == "john.doe@example.com"

    def test_invalid_gender_normalized(self, valid_user_data):
        """Test invalid gender is normalized to 'other'."""
        valid_user_data['gender'] = "unknown"
        user = UserAPIResponse.parse_obj(valid_user_data)
        assert user.gender == "other"

    def test_gender_case_normalized(self, valid_user_data):
        """Test gender case normalization."""
        valid_user_data['gender'] = "MALE"
        user = UserAPIResponse.parse_obj(valid_user_data)
        assert user.gender == "male"

    def test_email_case_normalized(self, valid_user_data):
        """Test email is lowercased."""
        valid_user_data['email'] = "John.Doe@EXAMPLE.COM"
        user = UserAPIResponse.parse_obj(valid_user_data)
        assert user.email == "john.doe@example.com"

    def test_invalid_email_raises_error(self, valid_user_data):
        """Test invalid email raises error."""
        valid_user_data['email'] = "not-an-email"
        with pytest.raises(ValidationError) as exc_info:
            UserAPIResponse.parse_obj(valid_user_data)
        assert "email" in str(exc_info.value).lower()

    def test_missing_required_field_raises_error(self, valid_user_data):
        """Test missing required field raises error."""
        del valid_user_data['name']
        with pytest.raises(ValidationError):
            UserAPIResponse.parse_obj(valid_user_data)


class TestTransformedUser:
    """Tests for TransformedUser model."""

    @pytest.fixture
    def valid_transformed_data(self, sample_transformed_user):
        return sample_transformed_user.copy()

    def test_valid_transformed_user(self, valid_transformed_data):
        """Test valid transformed user."""
        user = TransformedUser.parse_obj(valid_transformed_data)
        assert user.first_name == "John"
        assert user.email == "john.doe@example.com"

    def test_invalid_uuid_raises_error(self, valid_transformed_data):
        """Test invalid UUID raises error."""
        valid_transformed_data['id'] = "not-a-uuid"
        with pytest.raises(ValidationError):
            TransformedUser.parse_obj(valid_transformed_data)

    def test_uuid_format_validated(self, valid_transformed_data):
        """Test UUID format validation."""
        valid_transformed_data['id'] = "550e8400-e29b-41d4-a716-446655440000"
        user = TransformedUser.parse_obj(valid_transformed_data)
        assert user.id == "550e8400-e29b-41d4-a716-446655440000"

    def test_invalid_gender_raises_error(self, valid_transformed_data):
        """Test invalid gender raises error."""
        valid_transformed_data['gender'] = "unknown"
        with pytest.raises(ValidationError):
            TransformedUser.parse_obj(valid_transformed_data)

    def test_empty_first_name_raises_error(self, valid_transformed_data):
        """Test empty first name raises error."""
        valid_transformed_data['first_name'] = ""
        with pytest.raises(ValidationError):
            TransformedUser.parse_obj(valid_transformed_data)

    def test_name_too_long_raises_error(self, valid_transformed_data):
        """Test name too long raises error."""
        valid_transformed_data['first_name'] = "A" * 101
        with pytest.raises(ValidationError):
            TransformedUser.parse_obj(valid_transformed_data)

    def test_extra_fields_forbidden(self, valid_transformed_data):
        """Test extra fields are forbidden."""
        valid_transformed_data['extra_field'] = "not allowed"
        with pytest.raises(ValidationError):
            TransformedUser.parse_obj(valid_transformed_data)


class TestDLQMessage:
    """Tests for DLQMessage model."""

    def test_create_dlq_message(self):
        """Test creating a DLQ message."""
        message = DLQMessage(
            original_message='{"user": "data"}',
            error_type="ValidationError",
            error_message="Invalid email",
            timestamp="2024-01-15T10:30:00Z",
            retry_count=0,
            source_topic="user_data"
        )

        assert message.original_message == '{"user": "data"}'
        assert message.error_type == "ValidationError"
        assert message.retry_count == 0

    def test_from_error_creates_correct_message(self):
        """Test from_error() creates correct message."""
        original = {"user_id": "123", "email": "invalid"}
        error = ValueError("Invalid email format")

        message = DLQMessage.from_error(
            original_message=original,
            error=error,
            source_topic="user_data",
            retry_count=2
        )

        assert message.error_type == "ValueError"
        assert message.error_message == "Invalid email format"
        assert message.retry_count == 2
        assert message.source_topic == "user_data"

        # Original message should be JSON-serialized
        parsed = json.loads(message.original_message)
        assert parsed["user_id"] == "123"

    def test_from_error_handles_non_serializable(self):
        """Test from_error() handles non-serializable data."""
        original = {"datetime": datetime.now()}  # datetime is not directly JSON-serializable
        error = Exception("Test error")

        # Should not raise (uses default=str fallback)
        message = DLQMessage.from_error(
            original_message=original,
            error=error,
            source_topic="test_topic"
        )

        assert message.original_message is not None


class TestValidateAPIResponse:
    """Tests for validate_api_response() function."""

    def test_valid_response(self, sample_api_response):
        """Test valid response."""
        is_valid, data, errors = validate_api_response(sample_api_response)

        assert is_valid is True
        assert data is not None
        assert len(errors) == 0
        assert data.name.first == "John"

    def test_not_dict_response(self):
        """Test non-dict response."""
        is_valid, data, errors = validate_api_response("not a dict")

        assert is_valid is False
        assert data is None
        assert "not a dictionary" in errors[0]

    def test_missing_results_key(self):
        """Test missing 'results' key."""
        is_valid, data, errors = validate_api_response({"info": {}})

        assert is_valid is False
        assert "Missing 'results' key" in errors[0]

    def test_empty_results(self):
        """Test empty results."""
        is_valid, data, errors = validate_api_response({"results": []})

        assert is_valid is False
        assert "Empty results" in errors[0]

    def test_invalid_user_data(self):
        """Test invalid user data."""
        invalid_response = {
            "results": [{
                "gender": "male",
                # Missing required fields
            }]
        }

        is_valid, data, errors = validate_api_response(invalid_response)

        assert is_valid is False
        assert data is None
        assert len(errors) > 0


class TestValidateTransformedData:
    """Tests for validate_transformed_data() function."""

    def test_valid_data(self, sample_transformed_user):
        """Test valid data."""
        is_valid, data, errors = validate_transformed_data(sample_transformed_user)

        assert is_valid is True
        assert data is not None
        assert len(errors) == 0

    def test_invalid_uuid(self, sample_transformed_user):
        """Test invalid UUID."""
        sample_transformed_user['id'] = "invalid-uuid"
        is_valid, data, errors = validate_transformed_data(sample_transformed_user)

        assert is_valid is False
        assert len(errors) > 0

    def test_missing_field(self, sample_transformed_user):
        """Test missing field."""
        del sample_transformed_user['email']
        is_valid, data, errors = validate_transformed_data(sample_transformed_user)

        assert is_valid is False
        assert len(errors) > 0


class TestSparkUserSchema:
    """Tests for Spark User Schema."""

    def test_schema_has_required_fields(self):
        """Test schema contains required fields."""
        field_names = [field.name for field in SPARK_USER_SCHEMA.fields]

        required_fields = [
            'id', 'first_name', 'last_name', 'gender', 'address',
            'email', 'username', 'dob', 'registered_date', 'phone', 'picture'
        ]

        for field in required_fields:
            assert field in field_names, f"Missing field: {field}"

    def test_schema_field_types(self):
        """Test schema field types."""
        from pyspark.sql.types import StringType

        for field in SPARK_USER_SCHEMA.fields:
            assert isinstance(field.dataType, StringType), \
                f"Field {field.name} should be StringType"

    def test_nullable_fields(self):
        """Test nullable fields."""
        nullable_fields = {'dob'}  # Only dob is nullable

        for field in SPARK_USER_SCHEMA.fields:
            if field.name in nullable_fields:
                assert field.nullable is True, \
                    f"Field {field.name} should be nullable"
            else:
                assert field.nullable is False, \
                    f"Field {field.name} should not be nullable"
