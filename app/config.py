import logging
import os
from dataclasses import dataclass
from pathlib import Path

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
    lakebase_instance_name: str
    lakebase_db: str
    lakebase_port: int


def load_config() -> AppConfig:
    workspace_host = _with_scheme(
        os.getenv("DATABRICKS_HOST") or os.getenv("WORKSPACE_HOST") or ""
    )
    token = os.getenv("DATABRICKS_TOKEN", "") or get_auth_token()
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")
    if not warehouse_id:
        warehouse_id = "148ccb90800933a1"
        logger.warning("DATABRICKS_WAREHOUSE_ID not set, using hardcoded default: %s", warehouse_id)
    sql_http_path = os.getenv(
        "DATABRICKS_HTTP_PATH",
        f"/sql/1.0/warehouses/{warehouse_id}",
    )

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
        lakebase_instance_name=os.getenv("LAKEBASE_INSTANCE_NAME", ""),
        lakebase_db=os.getenv("LAKEBASE_DB_NAME", "iot_demo"),
        lakebase_port=int(os.getenv("LAKEBASE_DB_PORT", "5432")),
    )
