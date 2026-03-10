import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import requests
from databricks.sdk import WorkspaceClient

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent / ".env")
except ImportError:
    pass

logger = logging.getLogger(__name__)


def _with_scheme(host: str) -> str:
    if not host:
        return ""
    if host.startswith("http://") or host.startswith("https://"):
        return host
    return f"https://{host}"


def get_workspace_client() -> WorkspaceClient:
    profile = os.getenv("DATABRICKS_PROFILE")
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


def get_auth_token() -> str:
    auth_headers = get_workspace_client().config.authenticate()
    token = (auth_headers or {}).get("Authorization", "")
    return token.replace("Bearer ", "")


def _generate_lakebase_credential(workspace_host: str, token: str, instance_name: str) -> str:
    """Call Lakebase generate-database-credential API for a short-lived OAuth token."""
    url = f"{workspace_host.rstrip('/')}/api/2.0/database/credentials"
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        json={"request_id": "streamlit-app", "instance_names": [instance_name]},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json().get("token", "")


@dataclass
class AppConfig:
    app_title: str
    workspace_host: str
    token: str
    catalog: str
    schema: str
    warehouse_id: str
    sql_http_path: str
    genie_space_id: str
    dashboard_url: str
    refresh_seconds: int
    lakebase_host: str
    lakebase_port: int
    lakebase_db: str
    lakebase_user: str
    lakebase_password: str
    lakebase_instance_name: str


def load_config() -> AppConfig:
    workspace_host = _with_scheme(
        os.getenv("DATABRICKS_HOST") or os.getenv("WORKSPACE_HOST") or ""
    )
    token = os.getenv("DATABRICKS_TOKEN", "") or get_auth_token()
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "148ccb90800933a1")
    sql_http_path = os.getenv(
        "DATABRICKS_HTTP_PATH",
        f"/sql/1.0/warehouses/{warehouse_id}",
    )

    lakebase_password = os.getenv("LAKEBASE_DB_PASSWORD", "")
    lakebase_instance_name = os.getenv("LAKEBASE_INSTANCE_NAME", "")

    if not lakebase_password and lakebase_instance_name and workspace_host and token:
        try:
            lakebase_password = _generate_lakebase_credential(
                workspace_host, token, lakebase_instance_name,
            )
            logger.info("Generated Lakebase credential via generate-database-credential API")
        except Exception as exc:
            logger.warning("Failed to generate Lakebase credential: %s", exc)

    return AppConfig(
        app_title=os.getenv("APP_TITLE", "IoT Manufacturing Flow Break Command Center"),
        workspace_host=workspace_host,
        token=token,
        catalog=os.getenv("APP_CATALOG", "welch"),
        schema=os.getenv("APP_SCHEMA", "iot_demo_dev"),
        warehouse_id=warehouse_id,
        sql_http_path=sql_http_path,
        genie_space_id=os.getenv("APP_GENIE_SPACE_ID", "__AUTO__"),
        dashboard_url=os.getenv(
            "APP_DASHBOARD_URL",
            "https://adb-984752964297111.11.azuredatabricks.net/embed/dashboardsv3/01f11755a5c31ce2ae7e3f1c7a514115?o=984752964297111",
        ),
        refresh_seconds=int(os.getenv("APP_REFRESH_SECONDS", "15")),
        lakebase_host=os.getenv("LAKEBASE_DB_HOST", ""),
        lakebase_port=int(os.getenv("LAKEBASE_DB_PORT", "5432")),
        lakebase_db=os.getenv("LAKEBASE_DB_NAME", "iot_demo"),
        lakebase_user=os.getenv("LAKEBASE_DB_USER", ""),
        lakebase_password=lakebase_password,
        lakebase_instance_name=lakebase_instance_name,
    )
