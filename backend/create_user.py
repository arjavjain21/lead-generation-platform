#!/usr/bin/env python3
"""
Admin CLI for managing users of the Lead Generation Platform.

Usage
-----
# Create a regular user
python create_user.py create user@example.com password123

# Create an admin user
python create_user.py create admin@example.com password123 --admin

# Reset a user's password
python create_user.py reset-password user@example.com newpassword456

# List all users
python create_user.py list

Run from the backend/ directory.
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

import shared.auth as auth


def cmd_create(args: argparse.Namespace) -> None:
    auth.init_auth_db()
    try:
        user = auth.create_user(email=args.email, password=args.password, is_admin=args.admin)
        role = "admin" if user["is_admin"] else "user"
        print(f"Created {role}: {user['email']} (id: {user['user_id']})")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_reset_password(args: argparse.Namespace) -> None:
    auth.init_auth_db()
    user = auth.get_user_by_email(args.email)
    if user is None:
        print(f"Error: no user with email '{args.email}'", file=sys.stderr)
        sys.exit(1)
    try:
        auth.update_password(args.email, args.password)
        print(f"Password updated for {args.email}")
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_list(args: argparse.Namespace) -> None:
    auth.init_auth_db()
    users = auth.list_users()
    if not users:
        print("No users found.")
        return
    print(f"{'Email':<40} {'Admin':<8} {'User ID':<38} {'Created'}")
    print("-" * 100)
    for u in users:
        admin_str = "yes" if u["is_admin"] else "no"
        print(f"{u['email']:<40} {admin_str:<8} {u['user_id']:<38} {u['created_at']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage Lead Generation Platform users.")
    subparsers = parser.add_subparsers(dest="command")

    p_create = subparsers.add_parser("create", help="Create a new user.")
    p_create.add_argument("email")
    p_create.add_argument("password")
    p_create.add_argument("--admin", action="store_true", help="Grant admin privileges.")
    p_create.set_defaults(func=cmd_create)

    p_reset = subparsers.add_parser("reset-password", help="Reset a user's password.")
    p_reset.add_argument("email")
    p_reset.add_argument("password")
    p_reset.set_defaults(func=cmd_reset_password)

    p_list = subparsers.add_parser("list", help="List all users.")
    p_list.set_defaults(func=cmd_list)

    args = parser.parse_args()
    if not hasattr(args, "func"):
        parser.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
