import httpx
import logging
from typing import Optional

from app.core.config import settings
from app.schemas import SecurityMasterResponse, SecurityMasterDetail

logger = logging.getLogger(__name__)

def extract_base_symbol(instrument_token: str) -> str | None:
    """Extracts base symbol (e.g., 'SBIN' from 'NSE:SBIN-EQ')."""
    try:
        if ':' in instrument_token:
            base = instrument_token.split(':', 1)[1]
        else:
            base = instrument_token
        if base.endswith(('-EQ', '-BE', '-SM', '-ST')):
             return base.rsplit('-', 1)[0]
        else:
            return base # Assume FUT/OPT symbols etc. are used directly
    except Exception as e:
        logger.error(f"Error extracting base symbol from '{instrument_token}': {e}")
        return None

class SecurityMasterClient:
    def __init__(self):
        self.client = httpx.AsyncClient(
            base_url=settings.SECURITY_MASTER_URL,
            timeout=10.0
        )
        logger.info(f"Initialized SecurityMasterClient with base URL: {settings.SECURITY_MASTER_URL}")

    async def get_security_id_from_token(self, instrument_token: str) -> int | None:
        """
        Calls the Security Master API using the extracted base symbol in the path
        and returns ONLY the security ID (int) if successful, otherwise None.
        """
        lookup_symbol = extract_base_symbol(instrument_token)
        if not lookup_symbol:
            logger.error(f"Could not extract a usable symbol from instrument_token: {instrument_token}")
            return None

        # --- CORRECTED: Use Path Parameter ---
        # Define the endpoint path using an f-string to include the symbol
        # Adjust "/api/get/" if the base path is different
        endpoint = f"/api/alfagrow/security/get/{lookup_symbol}"
        # No query parameters needed
        params = None
        # ------------------------------------

        try:
            logger.debug(f"Calling Security Master: GET {endpoint} (original token: {instrument_token})")
            # --- CORRECTED: Call GET with only the formatted endpoint string ---
            response = await self.client.get(endpoint)
            # -----------------------------------------------------------------

            # Check for non-success status codes first
            if not response.is_success:
                # Specific handling for 404
                if response.status_code == 404:
                     logger.warning(f"Security Master returned 404 Not Found for symbol: {lookup_symbol} (token: {instrument_token})")
                else:
                     logger.error(f"Security Master returned non-success status {response.status_code} for symbol {lookup_symbol}. Response: {response.text}")
                return None # Indicate lookup failure for any non-success status

            # Parse the JSON response if successful
            response_json = response.json()

            # --- Parse the correct response structure ---
            try:
                validated_response = SecurityMasterResponse.model_validate(response_json)

                if validated_response.status == "success" and validated_response.count > 0 and validated_response.data:
                    security_id = validated_response.data[0].id
                    logger.info(f"Successfully resolved {instrument_token} to security_id: {security_id}")
                    return security_id
                else:
                    logger.warning(f"Security Master reported status '{validated_response.status}' or count={validated_response.count} for symbol: {lookup_symbol} (token: {instrument_token})")
                    return None # Indicate lookup failure (found nothing)

            except Exception as pydantic_error:
                logger.error(f"Security Master response validation failed for symbol {lookup_symbol}: {pydantic_error}. Response: {response.text}")
                return None # Indicate lookup failure (bad response format)

        except httpx.TimeoutException:
            logger.error(f"Timeout calling Security Master for symbol: {lookup_symbol}")
            return None
        except httpx.RequestError as e:
            logger.error(f"Network error calling Security Master for symbol {lookup_symbol}: {e}")
            return None

# Singleton instance
security_master_client = SecurityMasterClient()