from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from api_client import ApiClient

st.set_page_config(page_title="MonteCarlo Greenfield", layout="wide")
st.title("🎯 MonteCarlo Greenfield")
st.caption("Neustart ohne Migration – UI als Thin Client")

client = ApiClient()

with st.sidebar:
    st.subheader("API")
    api_url = st.text_input("Base URL", value=client.base_url)
    client = ApiClient(api_url)
    if st.button("Health Check", use_container_width=True):
        try:
            health = client.health()
            st.success(f"API status: {health.get('status', 'unknown')}")
        except Exception as exc:
            st.error(str(exc))

st.divider()
col1, col2, col3 = st.columns(3)
base_duration = col1.number_input("Base Duration (days)", min_value=1.0, value=120.0, step=1.0)
runs = int(col2.number_input("Runs", min_value=1000, max_value=100000, value=10000, step=1000))
sigma = col3.slider("Uncertainty (sigma)", min_value=0.05, max_value=0.50, value=0.15, step=0.01)

if st.button("🚀 Simulate", type="primary", use_container_width=True):
    try:
        result = client.simulate(base_duration_days=base_duration, runs=runs, sigma=sigma)
        m1, m2, m3 = st.columns(3)
        m1.metric("P85", f"{result['p85_days']} days")
        m2.metric("Mean", f"{result['mean_days']:.1f} days")
        m3.metric("Range", f"{result['min_days']} - {result['max_days']} days")

        sample = result.get("durations_sample", [])
        if sample:
            fig = go.Figure(go.Histogram(x=sample, nbinsx=40))
            fig.update_layout(template="plotly_white", xaxis_title="Duration (days)", yaxis_title="Frequency")
            st.plotly_chart(fig, use_container_width=True)

        pct_df = pd.DataFrame(result.get("percentiles", []))
        if not pct_df.empty:
            st.dataframe(pct_df, use_container_width=True, hide_index=True)

    except Exception as exc:
        st.error(str(exc))
