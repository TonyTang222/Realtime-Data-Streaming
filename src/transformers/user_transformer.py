"""User Data Transformation Logic"""

import uuid
import logging
from typing import Dict, Any

from src.utils.validators import (
    safe_get_nested,
    sanitize_string,
    sanitize_email,
    validate_required_fields
)
from src.config.schemas import TransformedUser, validate_transformed_data
from src.exceptions.custom_exceptions import TransformationError

logger = logging.getLogger(__name__)


class UserTransformer:
    """
    Transform raw API data into the target format.

    Usage::

        transformer = UserTransformer()
        try:
            result = transformer.transform(api_response['results'][0])
        except TransformationError as e:
            logger.error(f"Transformation failed: {e}")
            send_to_dlq(api_response, e)
    """

    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a single user record.

        Args:
            raw_data: Raw data from the randomuser.me API.

        Returns:
            Transformed data dict.

        Raises:
            TransformationError: If transformation fails.
        """
        try:
            # Extract and transform fields
            transformed = self._extract_fields(raw_data)

            # Validate transformation result
            is_valid, validated, errors = validate_transformed_data(transformed)
            if not is_valid:
                raise TransformationError(
                    f"Validation failed: {'; '.join(errors)}",
                    source_data=raw_data
                )

            logger.debug(
                "Successfully transformed user data",
                extra={
                    "user_id": transformed['id'],
                    "username": transformed['username']
                }
            )

            return transformed

        except TransformationError:
            # Re-raise as-is
            raise
        except Exception as e:
            # Wrap as TransformationError
            logger.error(
                f"Transformation failed: {e}",
                extra={"error_type": type(e).__name__}
            )
            raise TransformationError(
                f"Failed to transform user data: {e}",
                source_data=raw_data
            )

    def _extract_fields(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and transform fields from raw API data."""
        # Extract location sub-object
        location = raw_data.get('location', {})

        # Build address
        address = self._build_address(location)

        transformed = {
            'id': str(uuid.uuid4()),
            'first_name': sanitize_string(
                safe_get_nested(raw_data, 'name', 'first'),
                default="Unknown"
            ),
            'last_name': sanitize_string(
                safe_get_nested(raw_data, 'name', 'last'),
                default="Unknown"
            ),
            'gender': sanitize_string(
                raw_data.get('gender'),
                default="other"
            ).lower(),
            'address': address,
            'email': sanitize_email(raw_data.get('email')),
            'username': sanitize_string(
                safe_get_nested(raw_data, 'login', 'username'),
                default=""
            ),
            'dob': sanitize_string(
                safe_get_nested(raw_data, 'dob', 'date'),
                default=""
            ),
            'registered_date': sanitize_string(
                safe_get_nested(raw_data, 'registered', 'date'),
                default=""
            ),
            'phone': sanitize_string(
                raw_data.get('phone'),
                default=""
            ),
            'picture': sanitize_string(
                safe_get_nested(raw_data, 'picture', 'medium'),
                default=""
            ),
        }

        return transformed

    def _build_address(self, location: Dict[str, Any]) -> str:
        """
        Build a combined address string.

        API format::

            {
                "street": {"number": 123, "name": "Main St"},
                "city": "New York",
                "postcode": "10001",
                "state": "NY",
                "country": "USA"
            }

        Target format::

            "123 Main St, New York 10001, NY, USA"
        """
        street_number = safe_get_nested(location, 'street', 'number', default='')
        street_name = safe_get_nested(location, 'street', 'name', default='')
        city = sanitize_string(location.get('city', ''))
        postcode = sanitize_string(location.get('postcode', ''))
        state = sanitize_string(location.get('state', ''))
        country = sanitize_string(location.get('country', ''))

        parts = []

        # Street
        if street_number or street_name:
            street = f"{street_number} {street_name}".strip()
            parts.append(street)

        # City + Postcode
        if city:
            city_part = f"{city} {postcode}".strip() if postcode else city
            parts.append(city_part)

        # State
        if state:
            parts.append(state)

        # Country
        if country:
            parts.append(country)

        return ', '.join(parts)

