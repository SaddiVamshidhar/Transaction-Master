import logging
from app.schemas import SecurityMasterResponse, SecurityMasterData

logger = logging.getLogger(__name__)

# Updated mock DB to reflect the new, richer data structure
MOCK_SECURITY_DB = {
    "INFY": {
        "id": 123,
        "nse_symbol": "INFY",
        "company_name": "Infosys Limited",
    },
    "RELIANCE-EQ": {
        "id": 2002,
        "nse_symbol": "RELIANCE",
        "company_name": "Reliance Industries Ltd",
    },
    "TCS-EQ": {
        "id": 3003,
        "nse_symbol": "TCS",
        "company_name": "Tata Consultancy Services Ltd",
    }
}

class SecurityMasterClient:
    async def get_security_details(self, instrument_token: str) -> dict:
        """
        Simulates calling the real Security Master API.
        Returns a dictionary matching the API response structure.
        """
        security_info = MOCK_SECURITY_DB.get(instrument_token)

        if security_info:
            # Simulate a successful 200 OK response
            response_data = SecurityMasterData(**security_info)
            response = SecurityMasterResponse(success=True, data=response_data)
        else:
            # Simulate a 404 Not Found error response
            # In a real HTTP client, we'd return a dictionary representing the error JSON.
            # For the mock, we will just use the success=False flag.
            response = SecurityMasterResponse(success=False, data=None)
            logger.warning(f"Security not found for instrument_token: {instrument_token}")

        # Return the Pydantic model as a dictionary
        return response.model_dump()


# Singleton instance
security_master_client = SecurityMasterClient()