#!/usr/bin/env python3
"""
Simple MCP server that wraps Octoparse Python client.
This allows using Octoparse via MCP without OAuth.
"""
import json
import sys
import os
from pathlib import Path

# Add the project to path
sys.path.insert(0, '/var/www/lead-generation-platform')

from integrations.octoparse.octoparse_client import OctoparseClient

class OctoparseMCP:
    def __init__(self):
        self.client = OctoparseClient()

    def handle_request(self, method, params=None):
        """Handle MCP requests"""
        try:
            if method == 'initialize':
                return {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {}
                    },
                    "serverInfo": {
                        "name": "octoparse",
                        "version": "1.0.0"
                    }
                }

            elif method == 'tools/list':
                return {
                    "tools": [
                        {
                            "name": "get_task_groups",
                            "description": "Get all Octoparse task groups",
                            "inputSchema": {
                                "type": "object",
                                "properties": {}
                            }
                        },
                        {
                            "name": "search_tasks",
                            "description": "Search tasks in a task group",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "group_id": {"type": "string", "description": "Task group ID"}
                                },
                                "required": ["group_id"]
                            }
                        },
                        {
                            "name": "get_task_data",
                            "description": "Get scraped data from a task",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "task_id": {"type": "string", "description": "Task ID"},
                                    "offset": {"type": "integer", "default": 0},
                                    "size": {"type": "integer", "default": 10}
                                },
                                "required": ["task_id"]
                            }
                        },
                        {
                            "name": "find_emails_by_domain",
                            "description": "Find generic emails for a domain using existing scraped data",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "domain": {"type": "string", "description": "Domain to search (e.g., example.com)"}
                                },
                                "required": ["domain"]
                            }
                        }
                    ]
                }

            elif method == 'tools/call':
                tool_name = params.get('name')
                arguments = params.get('arguments', {})

                if tool_name == 'get_task_groups':
                    result = self.client.get_task_groups()
                    return {"content": [{"type": "text", "text": json.dumps(result)}]}

                elif tool_name == 'search_tasks':
                    group_id = arguments.get('group_id')
                    result = self.client.search_tasks(group_id)
                    return {"content": [{"type": "text", "text": json.dumps(result)}]}

                elif tool_name == 'get_task_data':
                    task_id = arguments.get('task_id')
                    offset = arguments.get('offset', 0)
                    size = arguments.get('size', 10)
                    result = self.client.get_data(task_id, offset=offset, size=size)
                    return {"content": [{"type": "text", "text": json.dumps(result)}]}

                elif tool_name == 'find_emails_by_domain':
                    domain = arguments.get('domain')
                    # Search through all existing scraped data for this domain
                    emails = self.find_emails_for_domain(domain)
                    return {"content": [{"type": "text", "text": json.dumps(emails)}]}

                else:
                    return {"error": f"Unknown tool: {tool_name}"}

            else:
                return {"error": f"Unknown method: {method}"}

        except Exception as e:
            return {"error": str(e)}

    def find_emails_for_domain(self, domain):
        """Search for emails for a specific domain in existing scraped data"""
        # Use the Contact Details Scraper task
        task_id = '6c4814c8-c380-4d6a-a7df-168e303bec6d'

        # Clean domain
        domain = domain.lower().strip()
        if domain.startswith('www.'):
            domain = domain[4:]

        results = []
        offset = 0
        size = 100

        while True:
            data = self.client.get_data(task_id, offset=offset, size=size)
            items = data.get('data', {}).get('data', [])

            if not items:
                break

            for item in items:
                item_domain = item.get('Domain', '').lower()
                if domain in item_domain or item_domain in domain:
                    emails = item.get('Emails', '')
                    if emails:
                        results.append({
                            'domain': item_domain,
                            'emails': emails,
                            'phones': item.get('Phones', ''),
                            'linkedin': item.get('LinkedIn', ''),
                            'facebook': item.get('Facebook', '')
                        })

            offset += size
            if offset >= data.get('data', {}).get('total', 0):
                break

        return results


def main():
    """Run the MCP server"""
    mcp = OctoparseMCP()

    # Read messages from stdin
    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                break

            request = json.loads(line)
            method = request.get('method')
            params = request.get('params', {})

            response = mcp.handle_request(method, params)

            # Send response
            print(json.dumps(response), flush=True)

        except Exception as e:
            print(json.dumps({"error": str(e)}), flush=True)


if __name__ == '__main__':
    main()
