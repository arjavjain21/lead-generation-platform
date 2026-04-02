# OctoParse Integration Documentation

## Overview

**OctoParse** is a web scraping platform that allows users to create and run web scraping tasks. This integration provides API access to:
- Start/stop cloud extraction tasks
- Get task status and results
- Manage task groups and individual tasks
- Retrieve extracted data

## What OctoParse Is

OctoParse is a **no-code web scraping tool** that enables users to:
- Visually scrape websites without writing code
- Run extractions in the cloud
- Export data in various formats (JSON, CSV, Excel, etc.)
- Schedule recurring extractions

## What It Is For (In This Project)

In the lead generation platform, OctoParse is used for:
1. **Enriching lead data** - Scraping additional information about companies/contacts
2. **Data extraction** - Extracting structured data from target websites
3. **Lead list building** - Gathering contact information from directories

## Authentication

### Method: Password Grant (Direct API Authentication)

Instead of OAuth (which requires browser callbacks), we use direct username/password authentication:

```
POST https://openapi.octoparse.com/token
Content-Type: application/json

{
    "username": "management@eagleinfoservice.com",
    "password": "hyperke$999",
    "grant_type": "password"
}
```

**Response:**
```json
{
    "data": {
        "access_token": "eyJ...",
        "expires_in": "86400",
        "token_type": "Bearer",
        "refresh_token": "..."
    }
}
```

### Token Management

- **Access Token**: Valid for 24 hours (86400 seconds)
- **Refresh Token**: Used to obtain new access tokens without re-authenticating
- **Auto-refresh**: The `OctoparseClient` automatically refreshes when token has <1 hour remaining

## API Endpoints

### Authentication

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/token` | POST | Obtain access token (password or refresh grant) |

### Cloud Extraction

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cloudextraction/start` | POST | Start a task |
| `/cloudextraction/stop` | POST | Stop a task |
| `/cloudextraction/statuses` | POST | Get task statuses |
| `/cloudextraction/statuses/v2` | POST | Get detailed task statuses |
| `/cloudextraction/task/subtasks` | GET | Get subtask status |

### Data Retrieval

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/data/all` | GET | Get data by offset |
| `/data/lotno/all` | GET | Get data from specific batch |
| `/data/notexported` | GET | Get non-exported data |
| `/data/markexported` | POST | Mark data as exported |
| `/data/remove` | POST | Remove data |

### Task Management

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/taskGroup` | GET | Get all task groups |
| `/task/search` | GET | Search tasks in group |
| `/task/copy` | POST | Duplicate a task |
| `/task/moveToGroup` | POST | Move task to group |
| `/task/urls{file}` | POST | Update task URLs |
| `/task/getActions` | POST | Get action parameters |
| `/task/updateActionProperties` | POST | Update action parameters |
| `/task/updateLoopItems` | POST | Update loop items |
| `/task/updateTaskParameters` | POST | Update task parameters |

## Usage in Python Code

### Basic Usage

```python
from integrations.octoparse.octoparse_client import OctoparseClient

# Create client (loads credentials from /home/ubuntu/.config/octoparse/credentials.json)
client = OctoparseClient()

# Get task groups
groups = client.get_task_groups()
print(groups)

# Search tasks in a group
tasks = client.search_tasks("1754081")
print(tasks)

# Get task status
status = client.get_task_status(["task-id-here"])
print(status)

# Start a task
result = client.start_task("task-id-here")
print(result)

# Get extracted data
data = client.get_data("task-id-here", offset=0, size=100)
print(data)
```

### Using the Singleton

```python
from integrations.octoparse.octoparse_client import get_client

client = get_client()
result = client.get_task_status(["task-id"])
```

## File Structure

```
integrations/octoparse/
├── README.md                    # This file
├── octoparse_client.py          # Python client library
├── credentials.json.template    # Template for credentials
└── swagger.json                 # OpenAPI specification
```

## Credentials Location

**IMPORTANT**: Credentials are stored securely at:
```
/home/ubuntu/.config/octoparse/credentials.json
```

This file is NOT committed to version control. Use `credentials.json.template` as a reference.

### Credentials File Format
```json
{
    "access_token": "eyJ...",
    "refresh_token": "...",
    "expires_in": "86400",
    "token_type": "Bearer",
    "expires_at": "2026-03-15T14:30:46Z",
    "obtained_at": "2026-03-14T14:30:46Z"
}
```

## CLI Commands

```bash
# Check token status
python3 integrations/octoparse/octoparse_client.py

# Get task groups
python3 integrations/octoparse/octoparse_client.py groups

# Get task status
python3 integrations/octoparse/octoparse_client.py status <task_id>

# Start a task
python3 integrations/octoparse/octoparse_client.py start <task_id>

# Get data from task
python3 integrations/octoparse/octoparse_client.py data <task_id> [offset] [size]

# Refresh token manually
python3 integrations/octoparse/octoparse_client.py refresh

# First-time authentication
python3 integrations/octoparse/octoparse_client.py auth
```

## Current Configuration

- **API Base URL**: https://openapi.octoparse.com
- **Token Endpoint**: https://openapi.octoparse.com/token
- **Default Task Group**: "My Group" (ID: 1754081)
- **Account**: management@eagleinfoservice.com

## Error Handling

Common error codes:
- `Invalid.Grant` - Incorrect username or password
- `InvalidTaskId` - Invalid task ID
- `ServerError` - Internal server error

## Security Notes

1. Never commit credentials to version control
2. File permissions are set to 600 (owner read/write only)
3. Tokens are automatically refreshed before expiry
4. Refresh tokens allow long-lived sessions without re-authentication

## References

- Official API Docs: https://openapi.octoparse.com/
- OctoParse Website: https://www.octoparse.com/
- Swagger/OpenAPI Spec: See `swagger.json` in this directory
