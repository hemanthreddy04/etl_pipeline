"""Credentials without client libraries.

Google: on Cloud Run the metadata server hands out a token for the service account the service runs as.
Elsewhere, GOOGLE_OAUTH_ACCESS_TOKEN or `gcloud auth print-access-token` is used.
Secrets: a connection stores only the NAME of a secret. The value is read from an environment variable
with that name, or from Google Secret Manager, at the moment it is needed.
"""
import base64
import os
import subprocess
import threading
import time

import requests

from .config import settings

_lock = threading.Lock()
_token = {"value": None, "expires": 0}
METADATA = "http://metadata.google.internal/computeMetadata/v1"


def gcp_token():
    with _lock:
        if _token["value"] and _token["expires"] - 60 > time.time():
            return _token["value"]
        static = os.environ.get("GOOGLE_OAUTH_ACCESS_TOKEN")
        if static:
            return static
        try:
            r = requests.get(f"{METADATA}/instance/service-accounts/default/token",
                             headers={"Metadata-Flavor": "Google"}, timeout=3)
            if r.ok:
                j = r.json()
                _token.update(value=j["access_token"], expires=time.time() + int(j.get("expires_in", 300)))
                return _token["value"]
        except requests.RequestException:
            pass
        try:
            out = subprocess.run(["gcloud", "auth", "print-access-token"], capture_output=True, text=True, timeout=20)
            if out.returncode == 0 and out.stdout.strip():
                _token.update(value=out.stdout.strip(), expires=time.time() + 1800)
                return _token["value"]
        except (OSError, subprocess.SubprocessError):
            pass
    raise RuntimeError("No Google credentials found. On Cloud Run the service account is used automatically; "
                       "locally run `gcloud auth login` or set GOOGLE_OAUTH_ACCESS_TOKEN.")


def gcp_headers():
    return {"Authorization": f"Bearer {gcp_token()}"}


def default_gcp_project():
    if settings.gcp_project:
        return settings.gcp_project
    try:
        r = requests.get(f"{METADATA}/project/project-id", headers={"Metadata-Flavor": "Google"}, timeout=3)
        if r.ok:
            return r.text.strip()
    except requests.RequestException:
        pass
    return ""


def secret_status(name):
    """Where a secret reference resolves, without revealing it."""
    if not name:
        return "none"
    if os.environ.get(name) or os.environ.get(name.upper().replace("-", "_")):
        return "environment"
    try:
        read_secret(name)
        return "secret manager"
    except Exception:
        return "missing"


def read_secret(name):
    if not name:
        raise RuntimeError("This connection has no secret reference")
    for key in (name, name.upper().replace("-", "_")):
        if os.environ.get(key):
            return os.environ[key]
    project = default_gcp_project()
    if not project:
        raise RuntimeError(f"Secret '{name}' is not set as an environment variable, and no GCP project is known for Secret Manager")
    r = requests.get(f"https://secretmanager.googleapis.com/v1/projects/{project}/secrets/{name}/versions/latest:access",
                     headers=gcp_headers(), timeout=20)
    if not r.ok:
        raise RuntimeError(f"Secret '{name}' could not be read from Secret Manager ({r.status_code})")
    return base64.b64decode(r.json()["payload"]["data"]).decode()
