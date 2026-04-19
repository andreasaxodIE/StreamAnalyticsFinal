"""
Streamlit Dashboard — Real-Time Food Delivery Analytics

Reads Parquet datasets produced by spark/run_all_ucs.py and displays
live-updating charts for all 7 use cases.

Reads from the same OUTPUT_BASE that Spark writes to:
  - Local:  ./output_parquet/ (default)
  - Azure:  abfss://<container>@<account>.dfs.core.windows.net/...

Usage:
    streamlit run dashboard/app.py

For Azure reads, set:
    OUTPUT_BASE=abfss://...
    AZURE_STORAGE_ACCOUNT=<account>
    AZURE_STORAGE_ACCOUNT_KEY=<key>
(requires: pip install adlfs)
"""

import os
import sys
import time
import pandas as pd
import streamlit as st

# Import hard-coded Azure credentials from spark_session.py
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "spark"))
from spark_session import (  # noqa: E402
    AZURE_OUTPUT_PATH,
    AZURE_STORAGE_ACCOUNT,
    AZURE_STORAGE_ACCOUNT_KEY,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
# Default to the hard-coded Azure path. Override with OUTPUT_BASE=./output_parquet
# for local mode.
OUTPUT_BASE = os.environ.get("OUTPUT_BASE", AZURE_OUTPUT_PATH)
USING_AZURE = OUTPUT_BASE.startswith(("abfss://", "wasbs://"))

st.set_page_config(
    page_title="Food Delivery Analytics — Group 09",
    page_icon="🚴",
    layout="wide",
)


def _azure_storage_options():
    """Build fsspec/adlfs storage options from the hard-coded credentials."""
    return {
        "account_name": AZURE_STORAGE_ACCOUNT,
        "account_key": AZURE_STORAGE_ACCOUNT_KEY,
    }


# Translate the old CSV filenames the dashboard uses into parquet
# dataset directories produced by run_all_ucs.py.
_CSV_TO_PARQUET = {
    "uc1_order_volume.csv":     "uc1_order_volume",
    "uc3_prep_sla.csv":         "uc3_prep_sla",
    "uc4_weather.csv":          "uc4_weather",
    "uc7_anomalies.csv":        "uc7_anomalies",
    "uc9_supply_demand.csv":    "uc9_supply_demand",
    "uc10_processing_time.csv": "uc10_processing_time",
    "uc11_order_value.csv":     "uc11_order_value",
    "uc12_eta_accuracy.csv":    "uc12_eta_accuracy",
    "uc13_courier_productivity.csv": "uc13_courier_productivity",
}


def load_csv(filename):
    """Load a Parquet dataset (keeps the name for call-site compatibility).

    Returns None if the dataset doesn't exist yet or is empty.
    """
    dataset = _CSV_TO_PARQUET.get(filename)
    if dataset is None:
        return None

    path = f"{OUTPUT_BASE.rstrip('/')}/{dataset}"

    try:
        if USING_AZURE:
            # adlfs handles abfss:// via fsspec
            df = pd.read_parquet(path, storage_options=_azure_storage_options())
        else:
            if not os.path.isdir(path):
                return None
            # Any .parquet files yet?
            has_data = any(
                f.endswith(".parquet")
                for _, _, files in os.walk(path)
                for f in files
            )
            if not has_data:
                return None
            df = pd.read_parquet(path)
    except Exception:
        return None

    return df if len(df) > 0 else None


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("🚴 Real-Time Food Delivery Analytics")
st.caption("Group 09 — BBADBA A | Streaming from Azure Event Hub → Spark → Dashboard")
st.caption(f"Reading Parquet from: `{OUTPUT_BASE}`" + (" (Azure)" if USING_AZURE else " (local)"))
st.divider()

# ---------------------------------------------------------------------------
# UC1 — Order Volume & Cancellation Rate
# ---------------------------------------------------------------------------
st.header("UC1 — Order volume & cancellation rate by zone")
df1 = load_csv("uc1_order_volume.csv")
if df1 is not None and len(df1) > 0:
    total_orders    = int(df1["total_orders"].sum())
    total_cancelled = int(df1["cancelled_orders"].sum())
    # Compute rate from totals, not average-of-rates (avoids Simpson's paradox).
    overall_rate = (total_cancelled / total_orders * 100) if total_orders else 0.0

    col1, col2, col3 = st.columns(3)
    col1.metric("Total orders", total_orders)
    col2.metric("Cancelled", total_cancelled)
    col3.metric("Cancellation rate", f"{overall_rate:.1f}%")

    st.bar_chart(df1.groupby("zone_id")[["total_orders", "cancelled_orders"]].sum())
else:
    st.info("Waiting for UC1 data...")

st.divider()

# ---------------------------------------------------------------------------
# UC3 — Prep SLA Breaches
# ---------------------------------------------------------------------------
st.header("UC3 — Peak-hour prep time SLA breaches")
df3 = load_csv("uc3_prep_sla.csv")
if df3 is not None and len(df3) > 0:
    col1, col2 = st.columns(2)
    col1.metric("Total breaches", int(df3["breach_count"].sum()))
    col2.metric("Avg prep time (breaches)", f"{df3['avg_prep_time_sec'].mean():.0f}s")

    peak_vs_off = df3.groupby("is_peak_hour")["breach_count"].sum().reset_index()
    peak_vs_off["is_peak_hour"] = peak_vs_off["is_peak_hour"].map({True: "Peak hour", False: "Off-peak"})
    st.bar_chart(peak_vs_off.set_index("is_peak_hour"))
else:
    st.info("Waiting for UC3 data...")

st.divider()

# ---------------------------------------------------------------------------
# UC4 — Weather Impact
# ---------------------------------------------------------------------------
st.header("UC4 — Weather impact on delivery times")
df4 = load_csv("uc4_weather.csv")
if df4 is not None and len(df4) > 0:
    weather_summary = df4.groupby("weather_condition").agg({
        "order_count": "sum",
        "avg_delivery_sec": "mean",
        "p95_delivery_sec": "mean",
    }).round(0)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Avg delivery time by weather")
        st.bar_chart(weather_summary["avg_delivery_sec"])
    with col2:
        st.subheader("P95 delivery time by weather")
        st.bar_chart(weather_summary["p95_delivery_sec"])
else:
    st.info("Waiting for UC4 data...")

st.divider()

# ---------------------------------------------------------------------------
# UC7 — Anomaly Detection
# ---------------------------------------------------------------------------
st.header("UC7 — Anomaly detection")
df7 = load_csv("uc7_anomalies.csv")
if df7 is not None and len(df7) > 0:
    anomaly_cols = ["impossible_speed", "location_jump", "offline_mid_delivery"]
    present_cols = [c for c in anomaly_cols if c in df7.columns]

    total_events    = int(df7["total_events"].sum())
    total_anomalies = int(df7["total_anomalies"].sum())
    # Compute rate from totals, not average-of-rates.
    overall_rate = (total_anomalies / total_events * 100) if total_events else 0.0

    col1, col2, col3 = st.columns(3)
    col1.metric("Total events scanned", total_events)
    col2.metric("Anomalies detected", total_anomalies)
    col3.metric("Anomaly rate", f"{overall_rate:.2f}%")

    if present_cols:
        anomaly_summary = df7.groupby("zone_id")[present_cols].sum()
        st.bar_chart(anomaly_summary)
else:
    st.info("Waiting for UC7 data...")

st.divider()

# ---------------------------------------------------------------------------
# UC9 — Supply vs Demand
# ---------------------------------------------------------------------------
st.header("UC9 — Supply vs demand imbalance by zone")
df9 = load_csv("uc9_supply_demand.csv")
if df9 is not None and len(df9) > 0:
    # Show the latest window per zone — summing courier counts across windows
    # double-counts the same couriers (they appear in every minute they're idle).
    latest9 = (
        df9.sort_values("window_start")
           .groupby("zone_id")
           .tail(1)
           .set_index("zone_id")
    )

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Demand vs supply by zone (latest window)")
        st.bar_chart(latest9[["demand_orders", "supply_couriers"]])
    with col2:
        st.subheader("Demand/supply ratio (latest window)")
        st.bar_chart(latest9["demand_supply_ratio"])
    st.caption(f"Latest window: {df9['window_end'].max()}")
else:
    st.info("Waiting for UC9 data...")

st.divider()

# ---------------------------------------------------------------------------
# UC10 — Avg Processing Time
# ---------------------------------------------------------------------------
st.header("UC10 — Avg prep time per zone")
df10 = load_csv("uc10_processing_time.csv")
if df10 is not None and len(df10) > 0:
    proc_by_zone = df10.groupby("zone_id").agg({
        "order_count": "sum",
        "avg_processing_sec": "mean",
        "max_processing_sec": "max",
    }).round(0)

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Avg processing time (seconds)")
        st.bar_chart(proc_by_zone["avg_processing_sec"])
    with col2:
        st.subheader("Orders processed")
        st.bar_chart(proc_by_zone["order_count"])
else:
    st.info("Waiting for UC10 data...")

st.divider()

# ---------------------------------------------------------------------------
# UC11 — Avg Order Value
# ---------------------------------------------------------------------------
st.header("UC11 — Average order value per zone")
df11 = load_csv("uc11_order_value.csv")
if df11 is not None and len(df11) > 0:
    val_by_zone = df11.groupby("zone_id").agg({
        "order_count": "sum",
        "avg_order_eur": "mean",
        "total_revenue_eur": "sum",
    }).round(2)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total orders", int(val_by_zone["order_count"].sum()))
    col2.metric("Avg basket size", f"€{val_by_zone['avg_order_eur'].mean():.2f}")
    col3.metric("Total revenue", f"€{val_by_zone['total_revenue_eur'].sum():,.2f}")

    st.bar_chart(val_by_zone[["avg_order_eur", "total_revenue_eur"]])
    st.caption("Total revenue accumulates over the demo — it grows with each closed window.")
else:
    st.info("Waiting for UC11 data...")

st.divider()

# ---------------------------------------------------------------------------
# UC12 — ETA Accuracy by zone × weather
# ---------------------------------------------------------------------------
st.header("UC12 — ETA prediction accuracy (zone × weather)")
df12 = load_csv("uc12_eta_accuracy.csv")
if df12 is not None and len(df12) > 0:
    # Re-weight by order_count so cells with more orders count proportionally.
    def _weighted(group, val_col, weight_col="order_count"):
        w = group[weight_col]
        if w.sum() == 0:
            return 0.0
        return (group[val_col] * w).sum() / w.sum()

    summary = (
        df12.groupby(["zone_id", "weather_condition"])
            .apply(lambda g: pd.Series({
                "order_count":        int(g["order_count"].sum()),
                "mean_error_sec":     round(_weighted(g, "mean_error_sec"), 0),
                "mae_sec":            round(_weighted(g, "mae_sec"), 0),
                "underest_rate_pct":  round(_weighted(g, "underest_rate_pct"), 1),
            }), include_groups=False)
            .reset_index()
            .sort_values("mae_sec", ascending=False)
    )

    total_orders      = int(df12["order_count"].sum())
    overall_mae       = round(_weighted(df12, "mae_sec"), 0)
    overall_underest  = round(_weighted(df12, "underest_rate_pct"), 1)

    col1, col2, col3 = st.columns(3)
    col1.metric("Deliveries evaluated", total_orders)
    col2.metric("Overall MAE", f"{overall_mae:.0f}s")
    col3.metric("Underestimation rate", f"{overall_underest:.1f}%")

    st.subheader("Worst (zone × weather) combinations by MAE")
    st.dataframe(summary.head(10), use_container_width=True, hide_index=True)

    st.caption(
        "MAE = mean absolute error (seconds). "
        "Positive mean_error = platform underestimates delivery time. "
        "Underest. rate = % of orders where actual delivery was slower than estimate."
    )
else:
    st.info("Waiting for UC12 data...")

st.divider()

# ---------------------------------------------------------------------------
# UC13 — Courier Productivity by vehicle × zone
# ---------------------------------------------------------------------------
st.header("UC13 — Courier productivity (vehicle type × zone)")
df13 = load_csv("uc13_courier_productivity.csv")
if df13 is not None and len(df13) > 0:
    # Aggregate across all windows — weight productivity by session count.
    def _wavg(group, val_col, weight_col="session_count"):
        w = group[weight_col]
        if w.sum() == 0:
            return 0.0
        return (group[val_col] * w).sum() / w.sum()

    rollup = (
        df13.groupby(["vehicle_type", "zone_id"])
            .apply(lambda g: pd.Series({
                "sessions":             int(g["session_count"].sum()),
                "avg_deliveries":       round(_wavg(g, "avg_deliveries"), 2),
                "p50_deliveries":       round(g["p50_deliveries"].median(), 1),
                "p90_deliveries":       round(g["p90_deliveries"].median(), 1),
                "deliveries_per_hour":  round(_wavg(g, "deliveries_per_hour"), 2),
            }), include_groups=False)
            .reset_index()
    )

    col1, col2, col3 = st.columns(3)
    col1.metric("Sessions observed", int(rollup["sessions"].sum()))
    col2.metric("Avg deliveries/session", f"{_wavg(df13, 'avg_deliveries'):.2f}")
    col3.metric("Avg deliveries/hour", f"{_wavg(df13, 'deliveries_per_hour'):.2f}")

    # Pivot: productivity heatmap-ish view by vehicle × zone
    pivot = rollup.pivot(index="zone_id", columns="vehicle_type", values="deliveries_per_hour")
    st.subheader("Deliveries per hour, by zone × vehicle type")
    st.bar_chart(pivot)

    st.subheader("Full breakdown")
    st.dataframe(
        rollup.sort_values("deliveries_per_hour", ascending=False),
        use_container_width=True, hide_index=True,
    )
else:
    st.info("Waiting for UC13 data...")

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------
st.divider()
st.caption(f"Last refreshed: {time.strftime('%H:%M:%S')} | Auto-refreshes every 5 seconds")
time.sleep(5)
st.rerun()
