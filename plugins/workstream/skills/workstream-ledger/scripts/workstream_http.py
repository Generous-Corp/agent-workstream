#!/usr/bin/env python3
"""Verified TLS defaults shared by the dependency-free HTTP transports."""

from __future__ import annotations

import os
import ssl
import sys


def default_ssl_context() -> ssl.SSLContext:
    """Use verified TLS, repairing Framework Python's missing macOS CA link."""
    paths = ssl.get_default_verify_paths()
    if sys.platform == "darwin" and not paths.cafile and not paths.capath:
        for candidate in ("/etc/ssl/cert.pem", "/opt/homebrew/etc/openssl@3/cert.pem"):
            if os.path.isfile(candidate):
                return ssl.create_default_context(cafile=candidate)
    return ssl.create_default_context()
