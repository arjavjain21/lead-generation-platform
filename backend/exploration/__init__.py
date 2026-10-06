"""Data Explorer BFF — read-only proxy over the Contacts API data-explorer surface.

The platform never connects to the contacts Postgres directly: every request
flows over HTTPS to the Contacts API (leadsdatabase.cc) with a server-side
token. See exploration/explorer_routes.py.
"""
