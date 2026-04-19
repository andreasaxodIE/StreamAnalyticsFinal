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

st.subheader("Orders")
st.caption("How the platform is performing on order intake, value, and cancellation behaviour.")

# ---------------------------------------------------------------------------
# UC1 — Order Volume & Cancellation Rate (was UC1)
# ---------------------------------------------------------------------------
st.header("UC1 — Order volume & cancellation rate by zone")
st.caption(
    "Counts distinct orders per 1-minute window per zone, and tracks the "
    "cancellation rate. Answers: *where is demand concentrated, and where "
    "do orders fail most?*"
)
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

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Orders by zone**")
        st.bar_chart(df1.groupby("zone_id")[["total_orders", "cancelled_orders"]].sum())
    with col_b:
        st.markdown("**Orders over time (all zones)**")
        ts = df1.copy()
        ts["window_start"] = pd.to_datetime(ts["window_start"])
        ts = ts.groupby("window_start")[["total_orders", "cancelled_orders"]].sum().sort_index()
        st.line_chart(ts)
else:
    st.info("Waiting for UC1 data...")

st.divider()

# ---------------------------------------------------------------------------
# UC2 — Order value & revenue (was UC11)
# ---------------------------------------------------------------------------
st.header("UC2 — Order value & revenue by zone")
st.caption(
    "Tracks average basket size and cumulative revenue per zone. "
    "Answers: *which zones drive the most revenue, and what do customers spend there?*"
)
df2 = load_csv("uc11_order_value.csv")
if df2 is not None and len(df2) > 0:
    val_by_zone = df2.groupby("zone_id").agg({
        "order_count": "sum",
        "avg_order_eur": "mean",
        "total_revenue_eur": "sum",
    }).round(2)

    col1, col2, col3 = st.columns(3)
    col1.metric("Total orders", int(val_by_zone["order_count"].sum()))
    col2.metric("Avg basket size", f"€{val_by_zone['avg_order_eur'].mean():.2f}")
    col3.metric("Total revenue", f"€{val_by_zone['total_revenue_eur'].sum():,.2f}")

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Avg basket size by zone (€)**")
        st.bar_chart(val_by_zone["avg_order_eur"])
    with col_b:
        st.markdown("**Cumulative revenue over time (€)**")
        ts = df2.copy()
        ts["window_start"] = pd.to_datetime(ts["window_start"])
        ts = (ts.groupby("window_start")["total_revenue_eur"]
                .sum().sort_index().cumsum())
        st.area_chart(ts)
else:
    st.info("Waiting for UC2 data...")

st.divider()

# ===========================================================================
# Operations section
# ===========================================================================
st.subheader("Operations")
st.caption("Supply, demand, and time-to-prep — the operational health of fulfilment.")

# ---------------------------------------------------------------------------
# UC3 — Supply vs demand (was UC9)
# ---------------------------------------------------------------------------
st.header("UC3 — Supply vs demand imbalance by zone")
st.caption(
    "Joins placed orders against idle couriers per 1-minute window. "
    "Answers: *where is demand outstripping courier supply right now — i.e. "
    "where would we trigger surge pricing?*"
)
df3 = load_csv("uc9_supply_demand.csv")
if df3 is not None and len(df3) > 0:
    # Latest-window snapshot: summing courier counts across windows would
    # double-count the same couriers.
    latest = (
        df3.sort_values("window_start")
           .groupby("zone_id").tail(1).set_index("zone_id")
    )
    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Demand vs supply by zone (latest window)**")
        st.bar_chart(latest[["demand_orders", "supply_couriers"]])
    with col_b:
        st.markdown("**Demand/supply ratio over time**")
        ts = df3.copy()
        ts["window_start"] = pd.to_datetime(ts["window_start"])
        ts_pivot = ts.pivot_table(
            index="window_start", columns="zone_id",
            values="demand_supply_ratio", aggfunc="mean",
        ).sort_index()
        st.line_chart(ts_pivot)
    st.caption(f"Latest window: {df3['window_end'].max()}")
else:
    st.info("Waiting for UC3 data...")

st.divider()

# ---------------------------------------------------------------------------
# UC4 — Avg prep time per zone (was UC10)
# ---------------------------------------------------------------------------
st.header("UC4 — Average prep time per zone")
st.caption(
    "Average time restaurants take to prepare orders, by zone. "
    "Answers: *which zones have the slowest kitchens, and where do we need "
    "operational attention?*"
)
df4 = load_csv("uc10_processing_time.csv")
if df4 is not None and len(df4) > 0:
    proc_by_zone = df4.groupby("zone_id").agg({
        "order_count": "sum",
        "avg_processing_sec": "mean",
        "max_processing_sec": "max",
    }).round(0)

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Avg prep time by zone (seconds)**")
        st.bar_chart(proc_by_zone["avg_processing_sec"])
    with col_b:
        st.markdown("**Orders processed per zone**")
        st.bar_chart(proc_by_zone["order_count"])
else:
    st.info("Waiting for UC4 data...")

st.divider()

# ---------------------------------------------------------------------------
# UC5 — SLA Breaches (was UC3)
# ---------------------------------------------------------------------------
st.header("UC5 — Prep-time SLA breaches")
st.caption(
    "Flags orders where prep time exceeded 20 minutes — a real SLA breach — "
    "and splits them by peak-hour status. "
    "Answers: *does peak-hour pressure cause more breaches than off-peak?*"
)
df5 = load_csv("uc3_prep_sla.csv")
if df5 is not None and len(df5) > 0:
    col1, col2 = st.columns(2)
    col1.metric("Total breaches", int(df5["breach_count"].sum()))
    col2.metric("Avg prep time on breach", f"{df5['avg_prep_time_sec'].mean():.0f}s")

    col_a, col_b = st.columns(2)
    with col_a:
        peak_vs_off = df5.groupby("is_peak_hour")["breach_count"].sum().reset_index()
        peak_vs_off["is_peak_hour"] = peak_vs_off["is_peak_hour"].map(
            {True: "Peak hour", False: "Off-peak"}
        )
        st.markdown("**Breaches: peak hour vs off-peak**")
        st.bar_chart(peak_vs_off.set_index("is_peak_hour"))
    with col_b:
        st.markdown("**Breaches over time**")
        ts = df5.copy()
        ts["window_start"] = pd.to_datetime(ts["window_start"])
        ts = ts.groupby("window_start")["breach_count"].sum().sort_index()
        st.line_chart(ts)
else:
    st.info("Waiting for UC5 data...")

st.divider()

# ===========================================================================
# Quality section
# ===========================================================================
st.subheader("Quality")
st.caption("Delivery-time performance and the accuracy of the platform's own predictions.")

# ---------------------------------------------------------------------------
# UC6 — Weather Impact (was UC4)
# ---------------------------------------------------------------------------
st.header("UC6 — Weather impact on delivery times")
st.caption(
    "Breaks down delivery time by weather condition. "
    "Answers: *how much do rain and snow actually slow us down?*"
)
df6 = load_csv("uc4_weather.csv")
if df6 is not None and len(df6) > 0:
    # Weight per-window averages by order_count. Simple .mean() would let a
    # 1-order SNOW window count the same as a 50-order CLEAR window, making
    # rare-weather results very noisy.
    def _wavg(group, val_col, weight_col="order_count"):
        w = group[weight_col]
        if w.sum() == 0:
            return 0.0
        return (group[val_col] * w).sum() / w.sum()

    weather_summary = (
        df6.groupby("weather_condition")
           .apply(lambda g: pd.Series({
               "order_count":       int(g["order_count"].sum()),
               "avg_delivery_sec":  round(_wavg(g, "avg_delivery_sec"), 0),
               "p95_delivery_sec":  round(_wavg(g, "p95_delivery_sec"), 0),
           }), include_groups=False)
           .reindex(["CLEAR", "RAIN", "HEAVY_RAIN", "SNOW"])
           .dropna()
    )

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Avg delivery time by weather (seconds)**")
        st.bar_chart(weather_summary["avg_delivery_sec"])
    with col_b:
        st.markdown("**P95 delivery time by weather (seconds)**")
        st.bar_chart(weather_summary["p95_delivery_sec"])

    st.caption(
        "Averages are weighted by order volume per window. "
        "Each delivery's actual time is drawn from a distribution, so with "
        "small sample sizes (short demos) a weather bucket can occasionally "
        "look faster than it really is. As the demo runs longer, you should "
        "see CLEAR < RAIN < HEAVY_RAIN < SNOW emerge in the avg delivery bars "
        "— matching the weather multipliers in the generator (1.0 / 1.2 / 1.45 / 1.6)."
    )
    st.markdown("**Sample sizes**")
    st.dataframe(
        weather_summary.reset_index(),
        use_container_width=True, hide_index=True,
    )
else:
    st.info("Waiting for UC6 data...")

st.divider()

# ---------------------------------------------------------------------------
# UC7 — ETA prediction accuracy (was UC12)
# ---------------------------------------------------------------------------
st.header("UC7 — Estimated ETA by zone × weather")
st.caption(
    "Shows the delivery time the platform's ETA model *promises* customers, "
    "broken down by zone and weather condition. "
    "Answers: *what delivery times are we advertising, and how does the "
    "model react to weather?* Compare with UC6 to spot overconfidence."
)
df7 = load_csv("uc12_eta_accuracy.csv")
if df7 is not None and len(df7) > 0:
    # Weighted aggregation — each window contributes proportionally to its
    # order count, so sparse-weather rows don't dominate.
    def _weighted(group, val_col, weight_col="order_count"):
        w = group[weight_col]
        if w.sum() == 0:
            return 0.0
        return (group[val_col] * w).sum() / w.sum()

    summary = (
        df7.groupby(["zone_id", "weather_condition"])
            .apply(lambda g: pd.Series({
                "order_count":  int(g["order_count"].sum()),
                "avg_eta_sec":  round(_weighted(g, "avg_eta_sec"), 0),
                "p90_eta_sec":  round(_weighted(g, "p90_eta_sec"), 0),
            }), include_groups=False)
            .reset_index()
            .sort_values("avg_eta_sec", ascending=False)
    )

    total_orders = int(df7["order_count"].sum())
    overall_avg  = round(_weighted(df7, "avg_eta_sec"), 0)
    overall_p90  = round(_weighted(df7, "p90_eta_sec"), 0)

    col1, col2, col3 = st.columns(3)
    col1.metric("Orders with ETAs issued", total_orders)
    col2.metric("Overall avg ETA", f"{overall_avg:.0f}s")
    col3.metric("Overall P90 ETA", f"{overall_p90:.0f}s")

    # Pivot for grouped bar chart: rows=weather, columns=zone
    eta_pivot = summary.pivot_table(
        index="weather_condition", columns="zone_id",
        values="avg_eta_sec", aggfunc="mean",
    ).reindex(["CLEAR", "RAIN", "HEAVY_RAIN", "SNOW"]).dropna(how="all")

    st.markdown("**Promised ETA (seconds) — weather × zone**")
    st.bar_chart(eta_pivot)

    st.markdown("**Longest promised ETAs — sorted**")
    st.dataframe(summary.head(10), use_container_width=True, hide_index=True)

    st.caption(
        "ETAs come from the `estimated_delivery_time_seconds` field, "
        "computed when the order is PLACED. Bars rising from CLEAR → SNOW "
        "mean the model is correctly pricing weather risk into its promises."
    )
else:
    st.info("Waiting for UC7 data...")

st.divider()

# ===========================================================================
# Fleet section
# ===========================================================================
st.subheader("Fleet")
st.caption("Courier telemetry — anomalies, productivity, vehicle mix.")

# ---------------------------------------------------------------------------
# UC8 — Courier anomaly detection (was UC7)
# ---------------------------------------------------------------------------
st.header("UC8 — Courier anomaly detection")
st.caption(
    "Scans courier GPS and status events for impossible speeds, sudden "
    "location jumps, and couriers going offline mid-delivery. "
    "Answers: *is the fleet behaving within normal parameters, or do we "
    "have fraud/malfunction signals?*"
)
df8 = load_csv("uc7_anomalies.csv")
if df8 is not None and len(df8) > 0:
    anomaly_cols = ["impossible_speed", "location_jump", "offline_mid_delivery"]
    present_cols = [c for c in anomaly_cols if c in df8.columns]

    total_events    = int(df8["total_events"].sum())
    total_anomalies = int(df8["total_anomalies"].sum())
    overall_rate = (total_anomalies / total_events * 100) if total_events else 0.0

    col1, col2, col3 = st.columns(3)
    col1.metric("Events scanned", total_events)
    col2.metric("Anomalies detected", total_anomalies)
    col3.metric("Anomaly rate", f"{overall_rate:.2f}%")

    col_a, col_b = st.columns(2)
    with col_a:
        st.markdown("**Anomalies by zone (stacked by type)**")
        if present_cols:
            st.bar_chart(df8.groupby("zone_id")[present_cols].sum())
    with col_b:
        st.markdown("**Anomaly rate over time (%)**")
        ts = df8.copy()
        ts["window_start"] = pd.to_datetime(ts["window_start"])
        ts_rate = (
            ts.groupby("window_start")
              .apply(lambda g: (g["total_anomalies"].sum() / g["total_events"].sum() * 100)
                     if g["total_events"].sum() else 0.0,
                     include_groups=False)
              .sort_index()
        )
        st.line_chart(ts_rate)
else:
    st.info("Waiting for UC8 data...")

st.divider()

# ---------------------------------------------------------------------------
# UC9 — Courier Productivity (was UC13)
# ---------------------------------------------------------------------------
st.header("UC9 — Courier productivity (vehicle type × zone)")
st.caption(
    "Two-stage aggregation: takes each courier session's final delivery "
    "count, then rolls up by vehicle type and zone. "
    "Answers: *which vehicle type is most productive in each zone?*"
)
df9 = load_csv("uc13_courier_productivity.csv")
if df9 is not None and len(df9) > 0:
    def _wavg(group, val_col, weight_col="session_count"):
        w = group[weight_col]
        if w.sum() == 0:
            return 0.0
        return (group[val_col] * w).sum() / w.sum()

    rollup = (
        df9.groupby(["vehicle_type", "zone_id"])
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
    col2.metric("Avg deliveries/session", f"{_wavg(df9, 'avg_deliveries'):.2f}")
    col3.metric("Avg deliveries/hour", f"{_wavg(df9, 'deliveries_per_hour'):.2f}")

    pivot = rollup.pivot(index="zone_id", columns="vehicle_type", values="deliveries_per_hour")
    st.markdown("**Deliveries per hour — zone × vehicle type**")
    st.bar_chart(pivot)

    st.markdown("**Full breakdown**")
    st.dataframe(
        rollup.sort_values("deliveries_per_hour", ascending=False),
        use_container_width=True, hide_index=True,
    )
else:
    st.info("Waiting for UC9 data...")

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------
st.divider()
st.caption(f"Last refreshed: {time.strftime('%H:%M:%S')} | Auto-refreshes every 5 seconds")
time.sleep(5)
st.rerun()
