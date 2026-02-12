"""Data Schemas and Validation"""

import re
from typing import Optional, List, Tuple, Any, Dict
from datetime import datetime
from pydantic import BaseModel, Field

from pydantic import field_validator

# PySpark imports are optional (not available in Airflow container)
try:
    from pyspark.sql.types import StructType, StructField, StringType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    StructType = None
    StructField = None
    StringType = None


# ============================================================
# Pydantic Models for API Response Validation
# ============================================================

class StreetInfo(BaseModel):
    """Street information."""
    number: int
    name: str


class LocationInfo(BaseModel):
    """Location information."""
    street: StreetInfo
    city: str
    state: str
    country: str
    postcode: str

    @field_validator('postcode', mode='before')
    @classmethod
    def convert_postcode_to_string(cls, v):
        """Ensure postcode is string."""
        return str(v) if v is not None else ""


class NameInfo(BaseModel):
    """Name information."""
    first: str
    last: str


class LoginInfo(BaseModel):
    """Login information."""
    username: str


class DOBInfo(BaseModel):
    """Date of birth information."""
    date: str
    age: int


class RegisteredInfo(BaseModel):
    """Registration information."""
    date: str


class PictureInfo(BaseModel):
    """Profile picture information."""
    medium: str


class UserAPIResponse(BaseModel):
    """Validates user data from the randomuser.me API response."""
    gender: str
    name: NameInfo
    location: LocationInfo
    email: str
    login: LoginInfo
    dob: DOBInfo
    registered: RegisteredInfo
    phone: str
    picture: PictureInfo

    @field_validator('gender')
    @classmethod
    def validate_gender(cls, v: str) -> str:
        """Validate gender value."""
        allowed_values = {'male', 'female', 'other'}
        if v.lower() not in allowed_values:
            return 'other'
        return v.lower()

    @field_validator('email')
    @classmethod
    def validate_email_format(cls, v: str) -> str:
        """Validate email format."""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, v):
            raise ValueError(f'Invalid email format: {v}')
        return v.lower()


# ============================================================
# Pydantic Model for Transformed Data
# ============================================================

class TransformedUser(BaseModel):
    """Transformed user data schema for Kafka and Cassandra."""
    id: str = Field(..., min_length=36, max_length=36, description="UUID")
    first_name: str = Field(..., min_length=1, max_length=100)
    last_name: str = Field(..., min_length=1, max_length=100)
    gender: str = Field(..., description="Gender: male, female, or other")
    address: str = Field(..., min_length=1, max_length=500)
    email: str
    username: str = Field(..., min_length=1, max_length=100)
    dob: str = Field(..., description="ISO format date string")
    registered_date: str = Field(..., description="ISO format date string")
    phone: str = Field(..., max_length=50)
    picture: str = Field(..., description="URL to profile picture")

    @field_validator('gender')
    @classmethod
    def validate_gender(cls, v: str) -> str:
        """Validate gender value."""
        if v not in ('male', 'female', 'other'):
            raise ValueError(f'Invalid gender: {v}. Must be male, female, or other')
        return v

    @field_validator('email')
    @classmethod
    def validate_email(cls, v: str) -> str:
        """Validate email format."""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, v):
            raise ValueError(f'Invalid email format: {v}')
        return v.lower()

    @field_validator('id')
    @classmethod
    def validate_uuid_format(cls, v: str) -> str:
        """Validate UUID format."""
        uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        if not re.match(uuid_pattern, v.lower()):
            raise ValueError(f'Invalid UUID format: {v}')
        return v

    class Config:
        # Strict mode: reject undefined fields
        extra = 'forbid'


# ============================================================
# Spark Schema (for Kafka message parsing)
# ============================================================

# This schema must match TransformedUser fields

if PYSPARK_AVAILABLE:
    SPARK_USER_SCHEMA = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("dob", StringType(), True),               # optional
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])
else:
    SPARK_USER_SCHEMA = None


# ============================================================
# Cassandra DDL Templates
# ============================================================

CASSANDRA_CREATE_KEYSPACE_CQL = """
CREATE KEYSPACE IF NOT EXISTS {keyspace}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '{replication_factor}'}};
"""

CASSANDRA_CREATE_TABLE_CQL = """
CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
    id UUID PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    address TEXT,
    email TEXT,
    username TEXT,
    dob TEXT,
    registered_date TEXT,
    phone TEXT,
    picture TEXT
);
"""


# ============================================================
# Dead Letter Queue Schema
# ============================================================

class DLQMessage(BaseModel):
    """Dead Letter Queue message format for failed message handling."""
    original_message: str = Field(..., description="Original message as JSON string")
    error_type: str = Field(..., description="Exception class name")
    error_message: str = Field(..., description="Error message")
    timestamp: str = Field(..., description="ISO format timestamp")
    retry_count: int = Field(default=0, description="Number of retries attempted")
    source_topic: str = Field(..., description="Source topic")

    @classmethod
    def from_error(
        cls,
        original_message: Any,
        error: Exception,
        source_topic: str,
        retry_count: int = 0
    ) -> "DLQMessage":
        """Create a DLQ message from an error."""
        import json
        return cls(
            original_message=json.dumps(original_message, default=str),
            error_type=type(error).__name__,
            error_message=str(error),
            timestamp=datetime.utcnow().isoformat() + "Z",
            retry_count=retry_count,
            source_topic=source_topic
        )


# ============================================================
# Validation Helper Functions
# ============================================================

def validate_api_response(response: Dict[str, Any]) -> Tuple[bool, Optional[UserAPIResponse], List[str]]:
    """Validate API response. Returns (is_valid, validated_data, errors)."""
    errors = []

    # Check basic structure
    if not isinstance(response, dict):
        return False, None, ["Response is not a dictionary"]

    if 'results' not in response:
        return False, None, ["Missing 'results' key in response"]

    if not response['results'] or len(response['results']) == 0:
        return False, None, ["Empty results array"]

    # Validate with Pydantic
    user_data = response['results'][0]
    try:
        validated = UserAPIResponse.model_validate(user_data)
        return True, validated, []
    except Exception as e:
        # Parse Pydantic error details
        if hasattr(e, 'errors'):
            for error in e.errors():
                field_path = '.'.join(str(x) for x in error.get('loc', []))
                errors.append(f"Field '{field_path}': {error.get('msg', 'unknown error')}")
        else:
            errors.append(str(e))
        return False, None, errors


def validate_transformed_data(data: Dict[str, Any]) -> Tuple[bool, Optional[TransformedUser], List[str]]:
    """Validate transformed data. Returns (is_valid, validated_data, errors)."""
    errors = []
    try:
        validated = TransformedUser.model_validate(data)
        return True, validated, []
    except Exception as e:
        if hasattr(e, 'errors'):
            for error in e.errors():
                field_path = '.'.join(str(x) for x in error.get('loc', []))
                errors.append(f"Field '{field_path}': {error.get('msg', 'unknown error')}")
        else:
            errors.append(str(e))
        return False, None, errors
