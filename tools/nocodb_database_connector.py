"""
title: NocoDB Database Connector
author: Psyopsys.AI
author_url: https://megacity.online
version: 2.3.0
license: MIT
description: Connect AI agents to NocoDB databases with per-agent access control
required_open_webui_version: 0.3.9
requirements: requests

This is an OpenWebUI Python Tool that provides direct NocoDB API access.
Configure via Admin Panel -> Tools -> NocoDB Database Connector -> Valves

Valves Configuration:
  NOCODB_API_URL: NocoDB API base URL (e.g., http://clientname-nocodb:8080)
  NOCODB_API_TOKEN: NocoDB API authentication token
  AGENT_BASE_MAPPING: JSON mapping of agent IDs to NocoDB base IDs
                      Format: {"model-id": "base-id", "another-model": "another-base"}
"""

import requests
import json
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class Tools:
    """Multi-tenant secure NocoDB connector with automatic base_id detection"""

    class Valves(BaseModel):
        NOCODB_API_URL: str = Field(
            default="http://nocodb:8080",
            description="NocoDB API base URL"
        )
        NOCODB_API_TOKEN: str = Field(
            default="",
            description="NocoDB API authentication token",
        )
        AGENT_BASE_MAPPING: str = Field(
            default='{"agent-id": "base-id"}',
            description="JSON mapping of agent IDs to allowed NocoDB base IDs",
        )

    def __init__(self):
        self.valves = self.Valves()
        self._table_cache = {}

    def _get_base_id_for_agent(self, __model__: Optional[Dict] = None) -> Optional[str]:
        """Get the base_id that this agent is allowed to access"""
        if not __model__:
            return None

        try:
            if isinstance(__model__, dict):
                model_id = __model__.get("id")
            else:
                model_id = str(__model__)

            mapping = json.loads(self.valves.AGENT_BASE_MAPPING)
            return mapping.get(model_id)
        except Exception as e:
            print(f"Error getting base_id for agent: {e}")
            return None

    def _get_table_id(self, base_id: str, table_name: str) -> Optional[str]:
        """Get table_id from table name by querying the base metadata"""
        cache_key = f"{base_id}_{table_name}"

        if cache_key in self._table_cache:
            return self._table_cache[cache_key]

        url = f"{self.valves.NOCODB_API_URL}/api/v2/meta/bases/{base_id}/tables"
        headers = {
            "xc-token": self.valves.NOCODB_API_TOKEN,
            "Content-Type": "application/json",
        }

        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()

            for table in data.get("list", []):
                if (
                    table.get("table_name") == table_name
                    or table.get("title") == table_name
                ):
                    table_id = table.get("id")
                    self._table_cache[cache_key] = table_id
                    return table_id

            return None
        except Exception as e:
            print(f"Error getting table_id: {e}")
            return None

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make authenticated request to NocoDB API"""
        url = f"{self.valves.NOCODB_API_URL}{endpoint}"
        headers = {
            "xc-token": self.valves.NOCODB_API_TOKEN,
            "Content-Type": "application/json",
        }

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=data,
                params=params,
                timeout=30,
            )
            response.raise_for_status()
            return {"success": True, "data": response.json()}
        except requests.exceptions.RequestException as e:
            return {"success": False, "error": str(e)}

    def query_table(
        self,
        table_name: str,
        filters: Optional[str] = None,
        limit: int = 25,
        offset: int = 0,
        sort: Optional[str] = None,
        __model__: Optional[Dict] = None,
    ) -> str:
        """
        Query records from a NocoDB table. The database is automatically selected based on the agent.

        :param table_name: Name of the table to query
        :param filters: Filter conditions as JSON string (e.g., '{"where": "(City,eq,Charleston)"}')
        :param limit: Maximum number of records to return (default: 25)
        :param offset: Number of records to skip (default: 0)
        :param sort: Sort order (e.g., "-Occupancy" for descending)
        :return: JSON string with query results
        """
        base_id = self._get_base_id_for_agent(__model__)

        if not base_id:
            model_id = __model__.get("id") if isinstance(__model__, dict) else "unknown"
            return json.dumps(
                {
                    "success": False,
                    "error": f"No database configured for agent '{model_id}'. Please contact administrator to configure AGENT_BASE_MAPPING.",
                }
            )

        table_id = self._get_table_id(base_id, table_name)

        if not table_id:
            # List available tables to help user
            tables = self._list_available_tables(base_id)
            return json.dumps(
                {
                    "success": False,
                    "error": f"Table '{table_name}' not found in database.",
                    "available_tables": tables
                }
            )

        endpoint = f"/api/v2/tables/{table_id}/records"
        params = {"limit": limit, "offset": offset}

        if filters:
            try:
                filter_dict = (
                    json.loads(filters) if isinstance(filters, str) else filters
                )
                params.update(filter_dict)
            except:
                pass

        if sort:
            params["sort"] = sort

        result = self._make_request("GET", endpoint, params=params)
        return json.dumps(result, indent=2)

    def _list_available_tables(self, base_id: str) -> list:
        """Helper to list available tables in a base"""
        try:
            endpoint = f"/api/v2/meta/bases/{base_id}/tables"
            result = self._make_request("GET", endpoint)
            if result.get("success"):
                return [t.get("title") for t in result.get("data", {}).get("list", [])]
            return []
        except:
            return []

    def get_record(
        self, table_name: str, record_id: str, __model__: Optional[Dict] = None
    ) -> str:
        """
        Get a specific record by ID.

        :param table_name: Name of the table
        :param record_id: ID of the record to retrieve
        :return: JSON string with record data
        """
        base_id = self._get_base_id_for_agent(__model__)

        if not base_id:
            return json.dumps(
                {"success": False, "error": "No database configured for this agent"}
            )

        table_id = self._get_table_id(base_id, table_name)

        if not table_id:
            return json.dumps(
                {"success": False, "error": f"Table '{table_name}' not found"}
            )

        endpoint = f"/api/v2/tables/{table_id}/records/{record_id}"
        result = self._make_request("GET", endpoint)
        return json.dumps(result, indent=2)

    def create_record(
        self, table_name: str, data: str, __model__: Optional[Dict] = None
    ) -> str:
        """
        Create a new record in a table.

        :param table_name: Name of the table
        :param data: Record data as JSON string
        :return: JSON string with created record
        """
        base_id = self._get_base_id_for_agent(__model__)

        if not base_id:
            return json.dumps(
                {"success": False, "error": "No database configured for this agent"}
            )

        table_id = self._get_table_id(base_id, table_name)

        if not table_id:
            return json.dumps(
                {"success": False, "error": f"Table '{table_name}' not found"}
            )

        try:
            record_data = json.loads(data) if isinstance(data, str) else data
        except:
            return json.dumps({"success": False, "error": "Invalid JSON data format"})

        endpoint = f"/api/v2/tables/{table_id}/records"
        result = self._make_request("POST", endpoint, data=record_data)
        return json.dumps(result, indent=2)

    def update_record(
        self,
        table_name: str,
        record_id: str,
        data: str,
        __model__: Optional[Dict] = None,
    ) -> str:
        """
        Update an existing record.

        :param table_name: Name of the table
        :param record_id: ID of the record to update
        :param data: Updated fields as JSON string
        :return: JSON string with update result
        """
        base_id = self._get_base_id_for_agent(__model__)

        if not base_id:
            return json.dumps(
                {"success": False, "error": "No database configured for this agent"}
            )

        table_id = self._get_table_id(base_id, table_name)

        if not table_id:
            return json.dumps(
                {"success": False, "error": f"Table '{table_name}' not found"}
            )

        try:
            record_data = json.loads(data) if isinstance(data, str) else data
        except:
            return json.dumps({"success": False, "error": "Invalid JSON data format"})

        endpoint = f"/api/v2/tables/{table_id}/records/{record_id}"
        result = self._make_request("PATCH", endpoint, data=record_data)
        return json.dumps(result, indent=2)

    def delete_record(
        self, table_name: str, record_id: str, __model__: Optional[Dict] = None
    ) -> str:
        """
        Delete a record from a table.

        :param table_name: Name of the table
        :param record_id: ID of the record to delete
        :return: JSON string with deletion result
        """
        base_id = self._get_base_id_for_agent(__model__)

        if not base_id:
            return json.dumps(
                {"success": False, "error": "No database configured for this agent"}
            )

        table_id = self._get_table_id(base_id, table_name)

        if not table_id:
            return json.dumps(
                {"success": False, "error": f"Table '{table_name}' not found"}
            )

        endpoint = f"/api/v2/tables/{table_id}/records/{record_id}"
        result = self._make_request("DELETE", endpoint)
        return json.dumps(result, indent=2)

    def list_tables(self, __model__: Optional[Dict] = None) -> str:
        """
        List all tables in the database.

        :return: JSON string with list of tables
        """
        base_id = self._get_base_id_for_agent(__model__)

        if not base_id:
            return json.dumps(
                {"success": False, "error": "No database configured for this agent"}
            )

        endpoint = f"/api/v2/meta/bases/{base_id}/tables"
        result = self._make_request("GET", endpoint)
        return json.dumps(result, indent=2)

    def get_table_schema(
        self, table_name: str, __model__: Optional[Dict] = None
    ) -> str:
        """
        Get schema/structure of a table.

        :param table_name: Name of the table
        :return: JSON string with table schema
        """
        base_id = self._get_base_id_for_agent(__model__)

        if not base_id:
            return json.dumps(
                {"success": False, "error": "No database configured for this agent"}
            )

        table_id = self._get_table_id(base_id, table_name)

        if not table_id:
            return json.dumps(
                {"success": False, "error": f"Table '{table_name}' not found"}
            )

        endpoint = f"/api/v2/meta/tables/{table_id}"
        result = self._make_request("GET", endpoint)
        return json.dumps(result, indent=2)
