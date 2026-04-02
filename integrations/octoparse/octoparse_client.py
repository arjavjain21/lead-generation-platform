#!/usr/bin/env python3
"""
OctoParse API Client for Lead Generation Platform
===================================================

This module provides integration with OctoParse for web scraping and data extraction.
It handles authentication, token refresh, and provides helper methods for common API operations.

PREREQUISITES:
- Credentials should be stored securely at: /home/ubuntu/.config/octoparse/credentials.json
- Or set environment variables: OCTOPARSE_USERNAME and OCTOPARSE_PASSWORD

USAGE:
    from octoparse_client import OctoparseClient

    client = OctoparseClient()
    result = client.get_task_status(["task-id-123"])
    print(result)
"""

import json
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, Dict, Any, List

import requests

# Configuration
CREDENTIALS_FILE = Path("/home/ubuntu/.config/octoparse/credentials.json")
TOKEN_URL = "https://openapi.octoparse.com/token"
API_BASE_URL = "https://openapi.octoparse.com"

# Token refresh threshold - refresh if less than this many seconds until expiry
REFRESH_THRESHOLD_SECONDS = 3600  # 1 hour


class OctoparseClient:
    """
    OctoParse API Client with automatic token management.

    Handles:
    - Authentication with username/password
    - Automatic token refresh before expiry
    - All OctoParse API endpoints
    """

    def __init__(self, credentials_file: Path = CREDENTIALS_FILE):
        self.credentials_file = credentials_file
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.expires_at: Optional[datetime] = None
        self.token_obtained_at: Optional[datetime] = None
        self._load_credentials()

    def _load_credentials(self):
        """Load credentials from secure file."""
        if self.credentials_file.exists():
            with open(self.credentials_file, "r") as f:
                data = json.load(f)
                self.access_token = data.get("access_token")
                self.refresh_token = data.get("refresh_token")
                expires_at_str = data.get("expires_at")
                if expires_at_str:
                    self.expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00"))
                self.token_obtained_at = data.get("obtained_at")

    def _save_credentials(self):
        """Save credentials to secure file."""
        self.credentials_file.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expires_in": "86400",
            "token_type": "Bearer",
            "expires_at": self.expires_at.isoformat().replace("+00:00", "Z") if self.expires_at else None,
            "obtained_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z") if self.token_obtained_at else None,
        }
        with open(self.credentials_file, "w") as f:
            json.dump(data, f, indent=4)
        os.chmod(self.credentials_file, 0o600)

    def _is_token_valid(self) -> bool:
        """Check if current token is valid and not about to expire."""
        if not self.access_token or not self.expires_at:
            return False
        now = datetime.now(timezone.utc)
        return (self.expires_at - now).total_seconds() > REFRESH_THRESHOLD_SECONDS

    def authenticate(self, username: str, password: str) -> Dict[str, Any]:
        """
        Authenticate with username/password and obtain new tokens.

        Args:
            username: OctoParse account email/username
            password: OctoParse account password

        Returns:
            Dict containing access_token, refresh_token, expires_in
        """
        response = requests.post(
            TOKEN_URL,
            headers={"Content-Type": "application/json"},
            json={
                "username": username,
                "password": password,
                "grant_type": "password",
            },
        )
        response.raise_for_status()
        data = response.json()["data"]

        self.access_token = data["access_token"]
        self.refresh_token = data["refresh_token"]
        expires_in = int(data["expires_in"])
        self.token_obtained_at = datetime.now(timezone.utc)
        self.expires_at = self.token_obtained_at + timedelta(seconds=expires_in)

        self._save_credentials()
        return data

    def refresh(self) -> Dict[str, Any]:
        """
        Refresh the access token using the refresh token.

        Returns:
            Dict containing new access_token, refresh_token, expires_in

        Raises:
            ValueError: If no refresh token is available
        """
        if not self.refresh_token:
            raise ValueError("No refresh token available. Please authenticate with username/password first.")

        response = requests.post(
            TOKEN_URL,
            headers={"Content-Type": "application/json"},
            json={
                "refresh_token": self.refresh_token,
                "grant_type": "refresh_token",
            },
        )
        response.raise_for_status()
        data = response.json()["data"]

        self.access_token = data["access_token"]
        if "refresh_token" in data:
            self.refresh_token = data["refresh_token"]
        expires_in = int(data["expires_in"])
        self.token_obtained_at = datetime.now(timezone.utc)
        self.expires_at = self.token_obtained_at + timedelta(seconds=expires_in)

        self._save_credentials()
        return data

    def ensure_valid_token(self):
        """
        Ensure we have a valid access token, refreshing if necessary.
        """
        if not self._is_token_valid():
            if self.refresh_token:
                print("Refreshing OctoParse token...")
                self.refresh()
            else:
                raise ValueError("No valid token and no refresh token. Please authenticate first.")

    def _headers(self) -> Dict[str, str]:
        """Get headers with authorization."""
        self.ensure_valid_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    # ==================== API Methods ====================

    def get_task_status(self, task_ids: List[str]) -> Dict[str, Any]:
        """
        Get status of one or more tasks.
        POST /cloudextraction/statuses

        Args:
            task_ids: List of task IDs to check

        Returns:
            Dict with task statuses
        """
        response = requests.post(
            f"{API_BASE_URL}/cloudextraction/statuses",
            headers=self._headers(),
            json={"taskIds": task_ids},
        )
        response.raise_for_status()
        return response.json()

    def get_task_status_v2(self, task_ids: List[str]) -> Dict[str, Any]:
        """
        Get detailed status of one or more tasks (V2).
        POST /cloudextraction/statuses/v2

        Returns additional fields: currentTotalExtractCount, executedTimes,
        subTaskCount, nextExecuteTime, endExecuteTime, startExecuteTime
        """
        response = requests.post(
            f"{API_BASE_URL}/cloudextraction/statuses/v2",
            headers=self._headers(),
            json={"taskIds": task_ids},
        )
        response.raise_for_status()
        return response.json()

    def start_task(self, task_id: str) -> Dict[str, Any]:
        """
        Start a cloud extraction task.
        POST /cloudextraction/start

        Args:
            task_id: The task ID to start

        Returns:
            Dict with lotNo and status
        """
        response = requests.post(
            f"{API_BASE_URL}/cloudextraction/start",
            headers=self._headers(),
            json={"taskId": task_id},
        )
        response.raise_for_status()
        return response.json()

    def stop_task(self, task_id: str) -> Dict[str, Any]:
        """
        Stop a cloud extraction task.
        POST /cloudextraction/stop
        """
        response = requests.post(
            f"{API_BASE_URL}/cloudextraction/stop",
            headers=self._headers(),
            json={"taskId": task_id},
        )
        response.raise_for_status()
        return response.json()

    def get_data(self, task_id: str, offset: int = 0, size: int = 100) -> Dict[str, Any]:
        """
        Get data from a task by offset.
        GET /data/all

        Args:
            task_id: Task ID to fetch data from
            offset: Data offset (0 = first row)
            size: Number of rows (1-1000)

        Returns:
            Dict with data array and pagination info
        """
        response = requests.get(
            f"{API_BASE_URL}/data/all",
            headers=self._headers(),
            params={"taskId": task_id, "offset": str(offset), "size": str(size)},
        )
        response.raise_for_status()
        return response.json()

    def get_data_by_lotno(self, task_id: str, lotno: str, offset: int = 0, size: int = 100) -> Dict[str, Any]:
        """
        Get data from a specific batch (lotno).
        GET /data/lotno/all
        """
        response = requests.get(
            f"{API_BASE_URL}/data/lotno/all",
            headers=self._headers(),
            params={"taskId": task_id, "lotno": lotno, "offset": str(offset), "size": str(size)},
        )
        response.raise_for_status()
        return response.json()

    def get_not_exported_data(self, task_id: str, size: int = 100) -> Dict[str, Any]:
        """
        Get non-exported data.
        GET /data/notexported
        """
        response = requests.get(
            f"{API_BASE_URL}/data/notexported",
            headers=self._headers(),
            params={"taskId": task_id, "size": str(size)},
        )
        response.raise_for_status()
        return response.json()

    def get_task_groups(self) -> Dict[str, Any]:
        """
        Get all task groups.
        GET /taskGroup
        """
        response = requests.get(
            f"{API_BASE_URL}/taskGroup",
            headers=self._headers(),
        )
        response.raise_for_status()
        return response.json()

    def search_tasks(self, task_group_id: str) -> Dict[str, Any]:
        """
        Search tasks in a task group.
        GET /task/search
        """
        response = requests.get(
            f"{API_BASE_URL}/task/search",
            headers=self._headers(),
            params={"taskGroupId": task_group_id},
        )
        response.raise_for_status()
        return response.json()

    def mark_data_exported(self, task_id: str) -> Dict[str, Any]:
        """
        Mark data as exported.
        POST /data/markexported
        """
        response = requests.post(
            f"{API_BASE_URL}/data/markexported",
            headers=self._headers(),
            json={"taskId": task_id},
        )
        response.raise_for_status()
        return response.json()


# Singleton instance for convenience
_client: Optional[OctoparseClient] = None


def get_client() -> OctoparseClient:
    """Get or create a singleton client instance."""
    global _client
    if _client is None:
        _client = OctoparseClient()
    return _client


# CLI for testing
if __name__ == "__main__":
    import sys

    client = OctoparseClient()

    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "auth":
            username = os.getenv("OCTOPARSE_USERNAME") or input("Username: ")
            password = os.getenv("OCTOPARSE_PASSWORD") or input("Password: ")
            result = client.authenticate(username, password)
            print("Authentication successful!")
            print(f"Access token: {result['access_token'][:50]}...")
            print(f"Expires in: {result['expires_in']} seconds")

        elif command == "refresh":
            result = client.refresh()
            print("Token refreshed!")
            print(f"Access token: {result['access_token'][:50]}...")

        elif command == "status":
            if len(sys.argv) < 3:
                print("Usage: python octoparse_client.py status <task_id>")
                sys.exit(1)
            result = client.get_task_status([sys.argv[2]])
            print(json.dumps(result, indent=2))

        elif command == "start":
            if len(sys.argv) < 3:
                print("Usage: python octoparse_client.py start <task_id>")
                sys.exit(1)
            result = client.start_task(sys.argv[2])
            print(json.dumps(result, indent=2))

        elif command == "data":
            if len(sys.argv) < 3:
                print("Usage: python octoparse_client.py data <task_id> [offset] [size]")
                sys.exit(1)
            offset = int(sys.argv[3]) if len(sys.argv) > 3 else 0
            size = int(sys.argv[4]) if len(sys.argv) > 4 else 10
            result = client.get_data(sys.argv[2], offset, size)
            print(json.dumps(result, indent=2))

        elif command == "groups":
            result = client.get_task_groups()
            print(json.dumps(result, indent=2))

        else:
            print(f"Unknown command: {command}")
            print("Available commands: auth, refresh, status, start, data, groups")
    else:
        if client._is_token_valid():
            if client.expires_at:
                remaining = (client.expires_at - datetime.now(timezone.utc)).total_seconds()
                print(f"Token is valid. Expires in {remaining/3600:.1f} hours.")
            else:
                print("Token is valid.")
        else:
            print("Token is invalid or expired.")
            print("Run 'python octoparse_client.py auth' to authenticate.")
