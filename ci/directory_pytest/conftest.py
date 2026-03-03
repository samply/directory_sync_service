import os
import time
from typing import Any, Dict, Optional

import pytest
import requests

EMX2_URL = os.environ.get("EMX2_URL", "http://emx2:8080").rstrip("/")
EMX2_EMAIL = os.environ.get("EMX2_EMAIL", "admin")
EMX2_PASSWORD = os.environ.get("EMX2_PASSWORD", "admin")

# Schemas used by tests (override in env if you ever rename in CI)
SCHEMA_BBMRI = os.environ.get("SCHEMA_BBMRI", "BBMRI-ERIC")
SCHEMA_ONTO = os.environ.get("SCHEMA_ONTO", "DirectoryOntologies")


def _wait_for_http_ok(url: str, timeout_s: int = 120) -> None:
    deadline = time.time() + timeout_s
    last_exc: Optional[Exception] = None
    while time.time() < deadline:
        try:
            r = requests.get(url, timeout=10)
            if r.status_code < 500:
                return
        except Exception as e:  # noqa: BLE001
            last_exc = e
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for {url}. Last error: {last_exc}")


def _signin(session: requests.Session) -> None:
    payload = {
        "query": (
            "mutation($email:String,$password:String){"
            "  signin(email:$email,password:$password){ status message token }"
            "}"
        ),
        "variables": {"email": EMX2_EMAIL, "password": EMX2_PASSWORD},
    }
    r = session.post(f"{EMX2_URL}/api/graphql", json=payload, timeout=30)
    r.raise_for_status()
    out = r.json()
    assert "errors" not in out, out
    res = out["data"]["signin"]
    assert res["status"] == "SUCCESS", out

    token = res.get("token")
    # If EMX2 returns a token, attach it for all subsequent requests
    if token:
        session.headers.update({"x-molgenis-token": token})


def _gql(session: requests.Session, schema: str, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    r = session.post(
        f"{EMX2_URL}/{schema}/graphql",
        json={"query": query, "variables": variables or {}},
        timeout=60,
    )
    r.raise_for_status()
    out = r.json()
    assert "errors" not in out, out
    return out["data"]


@pytest.fixture(scope="session")
def session() -> requests.Session:
    # Wait until the web server responds (import/sync services should already be done by compose ordering)
    _wait_for_http_ok(f"{EMX2_URL}/", timeout_s=180)

    s = requests.Session()
    _signin(s)
    return s


@pytest.fixture(scope="session")
def emx2(session):
    """
    Usage:
      data = emx2("BBMRI-ERIC", "query {...}", {"var": "x"})
    Returns the 'data' object of the GraphQL response.
    """
    def run(schema: str, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return _gql(session, schema, query, variables)

    return run

@pytest.fixture(scope="session")
def schema_names():
    return {"bbmri": SCHEMA_BBMRI, "onto": SCHEMA_ONTO}