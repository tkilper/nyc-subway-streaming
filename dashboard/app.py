import os
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(
    page_title="NYC Subway Live",
    page_icon="🚇",
    layout="wide",
)

DB = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    dbname=os.environ.get("POSTGRES_DB", "postgres"),
    user=os.environ.get("POSTGRES_USER", "postgres"),
    password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
)

STATUS_LABEL = {0: "Approaching", 1: "At Stop", 2: "In Transit"}
STATUS_ICON = {0: "🟡", 1: "🔴", 2: "🟢"}


def conn():
    return psycopg2.connect(**DB)


def fetch_trips() -> pd.DataFrame:
    sql = """
        WITH latest AS (
            SELECT DISTINCT ON (trip_id) *
            FROM trip_tracking
            ORDER BY trip_id, event_timestamp DESC
        )
        SELECT
            l.route_id,
            l.trip_id,
            l.current_stop_id,
            l.current_status,
            l.arrival_delay,
            l.departure_delay,
            l.event_timestamp,
            p.arrival_time AS scheduled_arrival
        FROM latest l
        LEFT JOIN LATERAL (
            SELECT arrival_time
            FROM processed_stop_updates
            WHERE trip_id = l.trip_id
              AND stop_id  = l.current_stop_id
            LIMIT 1
        ) p ON true
        ORDER BY l.route_id, l.trip_id
    """
    with conn() as c:
        return pd.read_sql(sql, c)


def fetch_anomalies() -> pd.DataFrame:
    sql = """
        SELECT route_id, trip_id, stop_id, arrival_delay,
               predicted_delay, residual, is_anomaly
        FROM trip_delay_anomalies
        ORDER BY residual DESC NULLS LAST
        LIMIT 500
    """
    with conn() as c:
        return pd.read_sql(sql, c)


def fmt_delay(seconds) -> str:
    if seconds is None or pd.isna(seconds):
        return "—"
    seconds = int(seconds)
    if seconds == 0:
        return "On time"
    m, s = divmod(abs(seconds), 60)
    label = "late" if seconds > 0 else "early"
    return f"{m}m {s}s {label}" if m else f"{s}s {label}"


def fmt_eta(scheduled_arrival, arrival_delay) -> str:
    if pd.isna(scheduled_arrival) or scheduled_arrival is None:
        return "—"
    estimated = int(scheduled_arrival) + int(arrival_delay or 0)
    delta = estimated - int(time.time())
    if abs(delta) < 30:
        return "Now"
    m = abs(delta) // 60
    return f"in {m}m" if delta > 0 else f"{m}m ago"


# ── Page header ──────────────────────────────────────────────────────────────

st.title("🚇 NYC MTA Subway Trip Tracking and Alerts - All Lines")

tab_trips, tab_anomaly = st.tabs(["Live Trips", "Anomaly Detection"])


# ── Live Trips ────────────────────────────────────────────────────────────────


@st.fragment(run_every=30)
def render_trips():
    st.caption(f"Refreshed at {datetime.now().strftime('%H:%M:%S')}")
    try:
        df = fetch_trips()
    except Exception as e:
        st.error(f"DB error: {e}")
        return

    if df.empty:
        st.info("No trip data yet — make sure the trip_tracking_job is running.")
        return

    # route filter (empty selection == all routes)
    all_routes = sorted(df["route_id"].dropna().unique().tolist())
    chosen = st.multiselect("Filter by route (leave empty for all)", all_routes, key="route_filter")
    if chosen:
        df = df[df["route_id"].isin(chosen)]
    if df.empty:
        st.info("No trips for the selected routes.")
        return

    # summary metrics
    c1, c2 = st.columns(2)
    c1.metric("Active Trips", len(df))
    c2.metric("Routes", df["route_id"].nunique())

    # route breakdown table
    st.markdown("#### Active Trips by Route")
    route_counts = (
        df.groupby("route_id")
        .size()
        .reset_index(name="Active Trips")
        .rename(columns={"route_id": "Route"})
        .sort_values("Active Trips", ascending=False)
    )
    st.dataframe(route_counts, use_container_width=True, hide_index=True)

    # trip table
    df["Status"] = df["current_status"].map(
        lambda s: f"{STATUS_ICON.get(s, '⚪')} {STATUS_LABEL.get(s, 'Unknown')}"
    )
    df["Delay"] = df["arrival_delay"].map(fmt_delay)
    df["Time to Stop"] = df.apply(
        lambda r: fmt_eta(r["scheduled_arrival"], r["arrival_delay"]), axis=1
    )
    display = df.rename(
        columns={
            "route_id": "Route",
            "trip_id": "Trip ID",
            "current_stop_id": "Current Stop",
        }
    )[["Route", "Trip ID", "Current Stop", "Status", "Delay", "Time to Stop"]]

    st.dataframe(display, use_container_width=True, hide_index=True)


# ── Anomaly Detection ─────────────────────────────────────────────────────────


@st.fragment(run_every=30)
def render_anomalies():
    st.caption(f"Refreshed at {datetime.now().strftime('%H:%M:%S')}")
    try:
        df = fetch_anomalies()
    except Exception as e:
        st.error(f"DB error: {e}")
        return

    if df.empty:
        st.info(
            "No anomaly data yet — the ARIMA model needs 50 observations per route before "
            "it starts scoring. Check that the anomaly_job is running."
        )
        return

    total = len(df)
    flagged = int(df["is_anomaly"].sum())
    scored = int(df["predicted_delay"].notna().sum())
    rate = f"{100 * flagged / scored:.1f}%" if scored else "—"

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Events", total)
    c2.metric("ARIMA Scored", scored)
    c3.metric("Anomalies", flagged)
    c4.metric("Anomaly Rate", rate)

    # anomalies per route chart
    if flagged:
        by_route = (
            df[df["is_anomaly"]]
            .groupby("route_id")
            .size()
            .reset_index(name="anomalies")
            .sort_values("anomalies", ascending=False)
        )
        fig = px.bar(
            by_route,
            x="route_id",
            y="anomalies",
            labels={"route_id": "Route", "anomalies": "Anomalies"},
            title="Anomalies by Route",
            color="anomalies",
            color_continuous_scale="Reds",
            height=280,
        )
        fig.update_layout(margin=dict(t=40, b=0, l=0, r=0), coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    # flagged events table
    st.markdown("#### 🚨 Flagged Anomalies")
    flagged_df = df[df["is_anomaly"]].copy()
    if flagged_df.empty:
        st.success("No anomalies in current data.")
    else:
        flagged_df["Delay"] = flagged_df["arrival_delay"].map(fmt_delay)
        flagged_df["Predicted"] = flagged_df["predicted_delay"].map(fmt_delay)
        flagged_df["Residual (s)"] = flagged_df["residual"].map(
            lambda x: f"{x:.1f}" if pd.notna(x) else "—"
        )
        st.dataframe(
            flagged_df.rename(
                columns={"route_id": "Route", "trip_id": "Trip ID", "stop_id": "Stop"}
            )[["Route", "Trip ID", "Stop", "Delay", "Predicted", "Residual (s)"]],
            use_container_width=True,
            hide_index=True,
        )

    # all scored events
    with st.expander("All scored events"):
        scored_df = df[df["predicted_delay"].notna()].copy()
        scored_df["🚨"] = scored_df["is_anomaly"].map(lambda x: "🚨" if x else "")
        scored_df["Delay"] = scored_df["arrival_delay"].map(fmt_delay)
        scored_df["Predicted"] = scored_df["predicted_delay"].map(fmt_delay)
        scored_df["Residual (s)"] = scored_df["residual"].map(
            lambda x: f"{x:.1f}" if pd.notna(x) else "—"
        )
        st.dataframe(
            scored_df.rename(
                columns={"route_id": "Route", "trip_id": "Trip ID", "stop_id": "Stop"}
            )[["🚨", "Route", "Trip ID", "Stop", "Delay", "Predicted", "Residual (s)"]],
            use_container_width=True,
            hide_index=True,
        )


with tab_trips:
    render_trips()

with tab_anomaly:
    render_anomalies()
