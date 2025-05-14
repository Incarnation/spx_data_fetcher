# dashboard/main.py
# =====================
# Multi‑Strategy Dashboard with unified PnL/Legs table, toggleable details.
# =====================
import logging
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd  # data manipulation

# Third-party imports
import pytz  # timezone handling
from dash import (  # Dash framework components
    MATCH,
    Dash,
    Input,
    Output,
    callback_context,
    dcc,
    html,
)
from flask_caching import Cache  # server-side caching
from plotly.graph_objects import Bar, Figure  # Plotly charting primitives

# ==============================================================================
# Constants
# ==============================================================================
EAST_TZ = pytz.timezone("US/Eastern")  # Eastern Time for display
CACHE_TIMEOUT = 300  # seconds for memoized queries

# ==============================================================================
# Environment Setup
# ==============================================================================
# Enable absolute imports & load .env for local development
sys.path.append(str(Path(__file__).resolve().parents[1]))
if not (os.getenv("RENDER") or os.getenv("RAILWAY_ENVIRONMENT")):
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

# ==============================================================================
# BigQuery utility functions
# ==============================================================================
from utils.bq_queries import (
    get_available_expirations,
    get_gamma_exposure_at_time,
    get_gamma_exposure_for_expiry,
    get_gamma_exposure_surface_data,
    get_historical_expirations,
    get_legs_data,
    get_live_pnl_data,
    get_trade_ids,
    get_trade_pl_analysis,
    get_trade_recommendations,
)

# ==============================================================================
# App Setup
# ==============================================================================
app = Dash(__name__, suppress_callback_exceptions=True)
app.title = "📊 Multi‑Strategy Trading Dashboard"
cache = Cache(app.server, config={"CACHE_TYPE": "SimpleCache"})


# ==============================================================================
# Memoized Fetchers
# ==============================================================================
@cache.memoize(timeout=CACHE_TIMEOUT)
def _expirations():
    """Returns list of upcoming expirations (YYYY-MM-DD)."""
    return list(reversed(get_available_expirations()))


@cache.memoize(timeout=CACHE_TIMEOUT)
def _trade_ids():
    """Returns all known trade IDs for MATCH callbacks."""
    return get_trade_ids()


@cache.memoize(timeout=CACHE_TIMEOUT)
def _hist_expirations():
    """Returns list of past expirations (YYYY‑MM‑DD)."""
    return get_historical_expirations(limit=100)


# ==============================================================================
# App Layout
# ==============================================================================
app.layout = html.Div(
    style={"fontFamily": "Arial", "maxWidth": "1200px", "margin": "auto", "padding": "20px"},
    children=[
        html.H1("📈 Multi‑Strategy Trading Dashboard", style={"textAlign": "center"}),
        dcc.Tabs(
            id="tabs",
            value="tab-gamma-surface",
            children=[
                dcc.Tab(label="Gamma Exposure Surface", value="tab-gamma-surface"),
                dcc.Tab(label="Gamma Exposure Analysis", value="tab-gamma"),
                dcc.Tab(label="Intraday Gamma Exposure", value="tab-gamma-intraday"),
                dcc.Tab(label="Trade Recommendations", value="tab-trades"),
            ],
        ),
        html.Div(id="tabs-content"),
    ],
)


# ==============================================================================
# Tab Content Renderers
# ==============================================================================
@app.callback(Output("tabs-content", "children"), Input("tabs", "value"))
def _render_tab(tab):
    if tab == "tab-gamma-surface":
        return _gamma_surface_tab()
    if tab == "tab-gamma":
        return _gamma_analysis_tab()
    if tab == "tab-gamma-intraday":
        return _gamma_intraday_tab()
    if tab == "tab-trades":
        return _trades_tab()
    return html.Div(f"Unknown tab: {tab}")


# --------------------------------
# Gamma Surface Tab
# --------------------------------
def _gamma_surface_tab():
    """3D surface with user-selectable expiry date range."""
    return html.Div(
        [
            html.H3("3D Gamma Exposure Surface"),
            html.Div(
                [
                    html.Label("Expiration Date Range:"),
                    dcc.DatePickerRange(
                        id="gamma-surface-date-range",
                        min_date_allowed=date.today(),
                        max_date_allowed=date.today() + timedelta(days=30),
                        start_date=date.today(),
                        end_date=date.today() + timedelta(days=30),
                        display_format="YYYY-MM-DD",
                    ),
                    html.Button("Refresh", id="refresh-gamma-surface", n_clicks=0),
                ],
                style={"display": "flex", "gap": "1rem", "marginBottom": "1rem"},
            ),
            dcc.Loading(dcc.Graph(id="gamma-surface-chart"), type="default"),
        ]
    )


@app.callback(
    Output("gamma-surface-chart", "figure"),
    Input("refresh-gamma-surface", "n_clicks"),
    Input("gamma-surface-date-range", "start_date"),
    Input("gamma-surface-date-range", "end_date"),
)
def _update_surface(n_clicks, start_date, end_date):
    """
    Refresh the 3D surface based on user-selected start/end expiration dates.
    """
    try:
        fig = get_gamma_exposure_surface_data(start_date=start_date, end_date=end_date)
        return fig
    except Exception as e:
        return Figure(layout={"title": f"Error loading surface: {e}"})


# --------------------------------
# Gamma Analysis Tab
# --------------------------------
def _gamma_analysis_tab():
    """
    Bar chart of net gamma exposure by strike for a selected expiry.
    """
    return html.Div(
        [
            html.H3("Gamma Exposure Analysis"),
            dcc.Dropdown(
                id="gamma-expiry-dropdown",
                options=[{"label": d, "value": d} for d in _expirations()],
                placeholder="Select Expiration Date",
                clearable=False,
                style={"width": "50%", "marginBottom": "1rem"},
            ),
            dcc.Loading(dcc.Graph(id="gamma-exposure-chart"), type="default"),
        ]
    )


@app.callback(
    Output("gamma-exposure-chart", "figure"),
    Input("gamma-expiry-dropdown", "value"),
)
def _update_gamma_chart(exp_date):
    """
    Produce bar chart of net gamma exposure for the selected expiration.
    """
    if not exp_date:
        return Figure(layout={"title": "Select an expiration date"})
    try:
        df, spot = get_gamma_exposure_for_expiry(exp_date)
    except Exception as e:
        return Figure(layout={"title": f"Error fetching data: {str(e)}"})

    if df.empty:
        return Figure(layout={"title": f"No data for {exp_date}"})

    fig = Figure()
    # Positive and negative bars for clarity
    fig.add_trace(
        Bar(
            x=df[df.net_gamma_exposure >= 0]["strike"],
            y=df[df.net_gamma_exposure >= 0]["net_gamma_exposure"],
            name="Positive GEX",
        )
    )
    fig.add_trace(
        Bar(
            x=df[df.net_gamma_exposure < 0]["strike"],
            y=df[df.net_gamma_exposure < 0]["net_gamma_exposure"],
            name="Negative GEX",
        )
    )
    # Spot price reference line
    if spot is not None:
        fig.add_shape(
            type="line",
            x0=spot,
            x1=spot,
            y0=df.net_gamma_exposure.min(),
            y1=df.net_gamma_exposure.max(),
            line=dict(color="black", dash="dash"),
        )
    # Zero-exposure line
    fig.add_shape(
        type="line",
        x0=df.strike.min(),
        x1=df.strike.max(),
        y0=0,
        y1=0,
        line=dict(color="gray", dash="dot"),
    )
    fig.update_layout(
        title=f"GEX on {exp_date}" + (f" | Spot≈{spot:.2f}" if spot else ""),
        xaxis_title="Strike",
        yaxis_title="Net Gamma Exposure",
        template="plotly_white",
        height=450,
    )
    return fig


# --------------------------------
# Trade Recommendations Tab
# --------------------------------
def _trades_tab():
    """
    List of trade recommendations with toggleable leg and P/L details.
    """
    return html.Div(
        [
            html.H3("Trade Recommendations", style={"textAlign": "center", "marginBottom": "1rem"}),
            dcc.Dropdown(
                id="trade-recommendation-status",
                options=[
                    {"label": s.capitalize(), "value": s} for s in ["pending", "active", "closed"]
                ],
                placeholder="Filter by status",
                clearable=True,
                style={"width": "300px", "margin": "auto 0 1rem 0"},
            ),
            dcc.Loading(html.Div(id="trade-recommendation-table"), type="default"),
        ]
    )


@app.callback(
    Output("trade-recommendation-table", "children"),
    Input("trade-recommendation-status", "value"),
)
def _update_trade_table(status_filter):
    """
    Build the trade table rows dynamically, including hidden detail divs
    for MATCH-based callbacks.
    """
    df = get_trade_recommendations(status_filter)
    if df.empty:
        return html.Div("No trades found.", style={"textAlign": "center", "marginTop": "2rem"})

    rows = []
    # Iterate each trade record
    for _, r in df.iterrows():
        # Convert entry_time to EST string
        dt = pd.to_datetime(r.entry_time)
        if dt.tzinfo is None:
            dt = pytz.utc.localize(dt)
        dt_est = dt.astimezone(EAST_TZ)
        entry_str = dt_est.strftime("%Y-%m-%d %H:%M") + " EST"

        # Main row components
        main = html.Div(
            [
                html.Div(f"ID: {r.trade_id}", className="trade-id"),
                html.Div(f"Strategy: {r.strategy_type}"),
                html.Div(f"Status: {r.status}"),
                html.Div(f"Entry: {entry_str}"),
                html.Div(
                    f"{'Exit:' if r.status=='closed' else 'PnL:'} ${(r.exit_price if r.status=='closed' else r.pnl):.2f}"
                ),
                # Expand/Collapse button, tracked by MATCH callback
                html.Button(
                    "Expand",
                    id={"type": "expand-button", "index": r.trade_id},
                    n_clicks=0,
                    className="expand-button",
                ),
            ],
            className="main-row",
        )

        # Hidden panel initially
        collapse = html.Div(
            id={"type": "collapse-content", "index": r.trade_id},
            style={"display": "none"},
            className="collapse-content",
        )

        # Wrap main + collapse in one row
        rows.append(html.Div([main, collapse], className="trade-row"))

    return html.Div(rows, className="trade-table")


@app.callback(
    # Update the style + contents of the collapse div, and button label
    Output({"type": "collapse-content", "index": MATCH}, "style"),
    Output({"type": "collapse-content", "index": MATCH}, "children"),
    Output({"type": "expand-button", "index": MATCH}, "children"),
    Input({"type": "expand-button", "index": MATCH}, "n_clicks"),
)
def _toggle_details(n_clicks):
    """
    Toggle visibility of the detail panel and update button label accordingly.
    """
    is_open = (n_clicks or 0) % 2 == 1  # odd clicks -> open
    btn_label = "Collapse" if is_open else "Expand"
    if not is_open:
        return {"display": "none"}, [], btn_label

    # Identify which trade id triggered the callback
    tid = callback_context.triggered_id["index"]

    # Fetch P/L analysis and leg data
    pl = get_trade_pl_analysis(tid).iloc[0]
    legs = get_legs_data(tid)
    live = get_live_pnl_data(tid)

    # Merge to combine static and live PnL
    merged = legs.merge(
        live[["leg_id", "current_price", "theoretical_pnl"]], on="leg_id", how="left"
    )

    # Compute total current PnL (closed uses stored pnl, open uses theoretical)
    total_pnl = (
        merged.apply(
            lambda row: row.pnl if row.status == "closed" else row.theoretical_pnl,
            axis=1,
        ).sum()
        * 100
    )

    # Build HTML table for each leg
    header = html.Tr(
        [html.Th(c) for c in ["Leg ID", "Type", "Dir", "Strike", "Entry", "Curr/Exit", "PnL"]]
    )
    leg_rows = []
    for _, l in merged.iterrows():
        price = l.exit_price if l.status == "closed" else l.current_price
        pnl = l.pnl if l.status == "closed" else l.theoretical_pnl
        leg_rows.append(
            html.Tr(
                [
                    html.Td(l.leg_id),
                    html.Td(l.leg_type),
                    html.Td(l.direction),
                    html.Td(f"{l.strike:.1f}"),
                    html.Td(f"${l.entry_price:.2f}"),
                    html.Td(f"${price:.2f}"),
                    html.Td(f"${pnl:.2f}"),
                ]
            )
        )
    table = html.Table([header] + leg_rows, className="legs-table")

    # Detail panel content
    details = [
        html.H5("P/L Analysis"),
        html.P(f"Max Profit: ${pl.max_profit:.2f}"),
        html.P(f"Max Loss:  ${pl.max_loss:.2f}"),
        html.P(f"Breakeven: {pl.breakeven_lower:.2f} – {pl.breakeven_upper:.2f}"),
        html.P(f"Prob. Profit: {pl.probability_profit:.1f}%"),
        html.H5(f"Current Total PnL: ${total_pnl:.2f}"),
        html.H5("Leg Details"),
        table,
    ]

    return {"display": "block"}, details, btn_label


# --------------------------------
# Intraday Gamma Exposure Tab
# --------------------------------
def _gamma_intraday_tab():
    return html.Div(
        [
            html.H3("Intraday Gamma Exposure (±5 min)"),
            html.Div(
                [
                    # Expiry selector
                    html.Label("Expiration Date:"),
                    dcc.Dropdown(
                        id="intraday-expiry-dropdown",
                        options=[{"label": d, "value": d} for d in _hist_expirations()],
                        placeholder="Select a past expiration",
                        style={"width": "200px", "marginRight": "1rem"},
                    ),
                    # Single EST time input
                    html.Label("Time (EST):"),
                    dcc.Input(
                        id="intraday-time-input",
                        type="text",
                        placeholder="2025-05-06 11:30:00",
                        style={"width": "200px", "marginRight": "1rem"},
                    ),
                    # Refresh button
                    html.Button("Refresh", id="refresh-intraday", n_clicks=0),
                ],
                style={
                    "display": "flex",
                    "alignItems": "center",
                    "gap": "1rem",
                    "marginBottom": "1rem",
                },
            ),
            # Graph placeholder
            dcc.Loading(dcc.Graph(id="intraday-gamma-chart"), type="default"),
        ]
    )


@app.callback(
    Output("intraday-gamma-chart", "figure"),
    Input("refresh-intraday", "n_clicks"),
    Input("intraday-expiry-dropdown", "value"),
    Input("intraday-time-input", "value"),
)
def _update_intraday_chart(n_clicks, expiry, time_str):
    """
    When the user clicks "Refresh":
      1) Parse the EST time string
      2) Convert to UTC TIMESTAMP literal
      3) Query get_gamma_exposure_at_time() ±5 min
      4) Aggregate net GEX by strike
      5) Draw a single Bar trace, red bars for negative GEX
    """
    # 1) Validate inputs: need both expiry and a time
    if not expiry or not time_str:
        return Figure(layout={"title": "Select expiration and time (EST)"})

    try:
        # 2) Parse input EST ↦ aware datetime
        dt_est = EAST_TZ.localize(datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S"))
        # 3) Convert EST ↦ UTC
        dt_utc = dt_est.astimezone(pytz.utc)
        # 4) Format as BigQuery TIMESTAMP string
        ts_utc = dt_utc.strftime("%Y-%m-%d %H:%M:%S")

        # 5) Fetch GEX within ±5 minutes
        df = get_gamma_exposure_at_time(
            snapshot_time=ts_utc,
            expiration_date=expiry,
            window_minutes=10,  # total window
        )

        # 6) If no data, show friendly message
        if df.empty:
            return Figure(layout={"title": f"No data around {time_str} EST"})

        # 7) Aggregate (in case multiple rows per strike)
        df_net = (
            df.groupby("strike", as_index=False)["net_gamma_exposure"].sum().sort_values("strike")
        )

        # 8) Build the bar colors: red if negative, else blue
        bar_colors = ["red" if g < 0 else "steelblue" for g in df_net["net_gamma_exposure"]]

        # 9) Create the bar chart
        fig = Figure()
        fig.add_trace(
            Bar(
                x=df_net["strike"],
                y=df_net["net_gamma_exposure"],
                name="Net GEX",
                marker=dict(color=bar_colors),
            )
        )

        # 10) Draw a horizontal zero line for reference
        fig.add_shape(
            type="line",
            x0=df_net.strike.min(),
            x1=df_net.strike.max(),
            y0=0,
            y1=0,
            line=dict(color="gray", dash="dot"),
        )

        # 11) Final layout touches
        fig.update_layout(
            title=(f"Intraday Net GEX on {expiry} at {dt_est.strftime('%H:%M')} ET (±5 min)"),
            xaxis_title="Strike",
            yaxis_title="Net Gamma Exposure",
            template="plotly_white",
            height=500,
        )
        return fig

    except Exception as e:
        logging.error(f"Error in intraday chart callback: {e}")
        return Figure(layout={"title": f"Error: {e}"})


# ==============================================================================
# Application Launch
# ==============================================================================
if __name__ == "__main__":
    # For production, disable debug=True and consider using a WSGI server
    app.run_server(debug=True, host="0.0.0.0", port=8050)
