# =====================
# dashboard/main.py
# Multi-Strategy Dashboard with Trade Recommendations, P/L Analysis, and Gamma Exposure
# =====================

import os
import sys
from pathlib import Path

from dotenv import load_dotenv

# Enable absolute imports
sys.path.append(str(Path(__file__).resolve().parents[1]))

# Load .env in dev
if not (os.getenv("RENDER") or os.getenv("RAILWAY_ENVIRONMENT")):
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

from dash import MATCH, Dash, Input, Output, callback, callback_context, dcc, html
from flask_caching import Cache
from plotly.graph_objects import Bar, Figure
from utils.bq_queries import (
    get_available_expirations,
    get_gamma_exposure_for_expiry,
    get_gamma_exposure_surface_data,
    get_legs_data,
    get_live_pnl_data,
    get_trade_ids,
    get_trade_pl_analysis,
    get_trade_recommendations,
)

# Init Dash app
app = Dash(__name__, suppress_callback_exceptions=True)
app.title = "📊 Multi-Strategy Trading Dashboard"
cache = Cache(app.server, config={"CACHE_TYPE": "SimpleCache"})


@cache.memoize(timeout=300)
def get_cached_expirations():
    return get_available_expirations()[::-1]


@cache.memoize(timeout=300)
def get_cached_trade_ids():
    return get_trade_ids()


# =====================
# App Layout with Tabs
# =====================
app.layout = html.Div(
    style={"fontFamily": "Arial", "maxWidth": "1200px", "margin": "auto", "padding": "20px"},
    children=[
        html.H1("📈 Multi-Strategy Trading Dashboard", style={"textAlign": "center"}),
        dcc.Tabs(
            id="tabs",
            value="tab-gamma-surface",
            children=[
                dcc.Tab(label="Gamma Exposure Surface", value="tab-gamma-surface"),
                dcc.Tab(label="Gamma Exposure Analysis", value="tab-gamma"),
                dcc.Tab(label="Trade Recommendations", value="tab-trades"),
            ],
        ),
        html.Div(id="tabs-content"),
    ],
)


# =====================
# Tab Content Renderer
# =====================
@callback(Output("tabs-content", "children"), Input("tabs", "value"))
def render_content(tab):
    if tab == "tab-gamma-surface":
        return render_gamma_surface_tab()
    elif tab == "tab-gamma":
        return render_gamma_exposure_tab()
    elif tab == "tab-trades":
        return render_trade_recommendations_tab()

    return html.Div("Invalid tab selected")


# =====================
# Tab 1: Gamma Exposure Surface
# =====================
def render_gamma_surface_tab():
    return html.Div(
        children=[
            html.H3("3D Gamma Exposure Surface"),
            html.Button("Refresh Data", id="refresh-gamma-surface"),
            dcc.Graph(id="gamma-surface-chart"),
        ]
    )


@callback(Output("gamma-surface-chart", "figure"), Input("refresh-gamma-surface", "n_clicks"))
def update_gamma_surface_chart(n_clicks):
    return get_gamma_exposure_surface_data()


# =====================
# Tab 2: Gamma Exposure Analysis
# =====================
def render_gamma_exposure_tab():
    return html.Div(
        children=[
            html.H3("Gamma Exposure Analysis"),
            dcc.Dropdown(
                id="gamma-expiry-dropdown",
                options=[{"label": exp, "value": exp} for exp in get_cached_expirations()],
                placeholder="Select Expiration Date",
            ),
            dcc.Graph(id="gamma-exposure-chart"),
        ]
    )


# =====================
# Tab: Trade Recommendations (Enhanced)
# =====================
def render_trade_recommendations_tab():
    return html.Div(
        children=[
            html.H3("Trade Recommendations", style={"textAlign": "center", "marginBottom": "20px"}),
            dcc.Dropdown(
                id="trade-recommendation-status",
                options=[
                    {"label": "Pending", "value": "pending"},
                    {"label": "Active", "value": "active"},
                    {"label": "Closed", "value": "closed"},
                ],
                placeholder="Select Trade Status",
                style={"width": "300px", "margin": "auto", "marginBottom": "20px"},
            ),
            html.Div(id="trade-recommendation-table"),
        ]
    )


@callback(
    Output("trade-recommendation-table", "children"),
    Input("trade-recommendation-status", "value"),
)
def update_trade_recommendation_table(status):
    trades_df = get_trade_recommendations(status)
    if trades_df.empty:
        return html.Div(
            "No trade recommendations found.",
            style={"textAlign": "center", "margin": "20px"},
        )

    rows = []
    for _, row in trades_df.iterrows():
        trade_id = row["trade_id"]

        main_row = html.Div(
            [
                html.Div(
                    [
                        html.Div(f"Trade ID: {trade_id}", className="trade-id"),
                        html.Div(f"Strategy: {row['strategy_type']}"),
                        html.Div(f"Status: {row['status']}"),
                        html.Div(f"Entry Price: ${row['entry_price']:.2f}"),
                        html.Div(f"PnL: ${row['pnl']:.2f}"),
                        html.Button(
                            "Expand",
                            id={"type": "expand-button", "index": trade_id},
                            n_clicks=0,
                            className="expand-button",
                        ),
                    ],
                    className="main-row",
                ),
                html.Div(
                    id={"type": "collapse-content", "index": trade_id},
                    className="collapse-content",
                    style={"display": "none"},
                ),
            ],
            className="trade-row",
        )
        rows.append(main_row)

    return html.Div(rows, className="trade-table")


@callback(
    Output({"type": "collapse-content", "index": MATCH}, "style"),
    Output({"type": "collapse-content", "index": MATCH}, "children"),
    Input({"type": "expand-button", "index": MATCH}, "n_clicks"),
    prevent_initial_call=True,
)
def toggle_trade_details(n_clicks):
    # which trade?
    tid = callback_context.triggered_id["index"]

    # 1) P/L Analysis
    pl = get_trade_pl_analysis(tid).iloc[0]

    # 2) Live PnL
    live = get_live_pnl_data(tid)
    total_live = live["theoretical_pnl"].sum() if not live.empty else 0.0

    # 3) Legs
    legs = get_legs_data(tid)

    details = [
        html.H5("P/L Analysis"),
        html.P(f"Max Profit:   ${pl.max_profit:.2f}"),
        html.P(f"Max Loss:     ${pl.max_loss:.2f}"),
        html.P(f"Breakeven:    ${pl.breakeven_lower:.2f} – ${pl.breakeven_upper:.2f}"),
        html.P(f"Prob. Profit: {pl.probability_profit:.1f}%"),
        html.P(f"Δ:            {pl.delta:.3f}"),
        html.P(f"Θ:            {pl.theta:.3f}"),
        html.H5("Live PnL"),
        html.P(f"Total Live PnL: ${total_live:.2f}", style={"fontWeight": "bold"}),
        html.Table(
            [html.Tr([html.Th(c) for c in ["Leg ID", "Curr Price", "PnL"]])]
            + [
                html.Tr(
                    [
                        html.Td(r.leg_id),
                        html.Td(f"${r.current_price:.2f}"),
                        html.Td(f"${r.theoretical_pnl:.2f}"),
                    ]
                )
                for _, r in live.iterrows()
            ],
            className="live-pnl-table",
        ),
        html.H5("Legs"),
        html.Table(
            [html.Tr([html.Th(c) for c in ["Leg ID", "Strike", "Dir", "Entry Price"]])]
            + [
                html.Tr(
                    [
                        html.Td(r.leg_id),
                        html.Td(f"{r.strike:.1f}"),
                        html.Td(r.direction),
                        html.Td(f"${r.entry_price:.2f}"),
                    ]
                )
                for _, r in legs.iterrows()
            ],
            className="legs-table",
        ),
    ]

    show = {"display": "block"} if (n_clicks or 0) % 2 == 1 else {"display": "none"}
    return show, details


@callback(Output("gamma-exposure-chart", "figure"), Input("gamma-expiry-dropdown", "value"))
def update_gamma_exposure_chart(expiration_date):
    if not expiration_date:
        return {"layout": {"title": "Select an expiration date"}}

    df, spot_price = get_gamma_exposure_for_expiry(expiration_date)
    if df.empty:
        return {"layout": {"title": f"No data for {expiration_date}"}}

    fig = Figure()

    fig.add_trace(
        Bar(
            x=df[df["net_gamma_exposure"] >= 0]["strike"],
            y=df[df["net_gamma_exposure"] >= 0]["net_gamma_exposure"],
            name="Positive GEX",
            marker_color="blue",
        )
    )

    fig.add_trace(
        Bar(
            x=df[df["net_gamma_exposure"] < 0]["strike"],
            y=df[df["net_gamma_exposure"] < 0]["net_gamma_exposure"],
            name="Negative GEX",
            marker_color="red",
        )
    )

    if spot_price:
        fig.add_shape(
            type="line",
            x0=spot_price,
            x1=spot_price,
            y0=df["net_gamma_exposure"].min(),
            y1=df["net_gamma_exposure"].max(),
            line=dict(color="black", dash="dash"),
        )

    fig.add_shape(
        type="line",
        x0=df["strike"].min(),
        x1=df["strike"].max(),
        y0=0,
        y1=0,
        line=dict(color="gray", dash="dot"),
    )

    fig.update_layout(
        title=(
            f"GEX on {expiration_date} | Spot ≈ {spot_price:.2f}"
            if spot_price
            else f"GEX on {expiration_date}"
        ),
        xaxis_title="Strike Price",
        yaxis_title="Net Gamma Exposure (Contracts × Gamma × 100)",
        yaxis_tickformat=",",
        template="plotly_white",
        height=500,
    )

    return fig


# =====================
# Main Entry Point
# =====================
if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
