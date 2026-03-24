import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd


@st.cache_resource
def get_bq_client():
    """
    Local dev: đọc credentials từ GOOGLE_APPLICATION_CREDENTIALS env var.
    VPS production: đọc từ credentials block trong secrets.toml.
    Hàm tự detect môi trường, không cần sửa code khi deploy.
    """
    if (
        "bigquery" in st.secrets
        and "credentials" in st.secrets.get("bigquery", {})
    ):
        creds_dict = dict(st.secrets["bigquery"]["credentials"])
        credentials = service_account.Credentials.from_service_account_info(
            creds_dict
        )
        return bigquery.Client(
            project=st.secrets["bigquery"]["project"],
            credentials=credentials,
        )
    return bigquery.Client(project=st.secrets["bigquery"]["project"])


@st.cache_data(ttl=300)
def query_bq(sql: str) -> pd.DataFrame:
    """
    Chạy SQL lên BigQuery, cache 5 phút.
    Dùng cho daily/historical data, không dùng cho realtime.
    """
    client = get_bq_client()
    return client.query(sql).to_dataframe()
