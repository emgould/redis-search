"""
Shared fixtures and utilities for OpenLibrary service tests.
"""

import os

import pytest


def pytest_configure(config):
    """Pytest hook to configure test environment before any tests run."""
    os.environ["ENVIRONMENT"] = "test"
