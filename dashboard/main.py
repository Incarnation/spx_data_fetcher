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

import pandas as pd
from dash import Dash, Input, Output, callback, dcc, html
from flask_caching import Cache
from plotly.graph_objects import Bar, Figure, Surface
from utils.bq_queries import (
    get_available_expirations,
    get_gamma_exposure_for_expiry,
    get_gamma_exposure_surface_data,
    get_live_pnl_data,
    get_trade_pl_analysis,
    get_trade_pl_projections,
    get_trade_recommendations,
)

# Init Dash app
app = Dash(__name__)
app.title = "📊 Multi-Strategy Trading Dashboard"
cache = Cache(app.server, config={"CACHE_TYPE": "SimpleCache"})


@cache.memoize(timeout=300)
def get_cached_expirations():
    return get_available_expirations()[::-1]


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
                dcc.Tab(label="Live PnL Monitoring", value="tab-pnl"),
                dcc.Tab(label="P/L Analysis & Projections", value="tab-pl-analysis"),
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
    elif tab == "tab-pnl":
        return render_live_pnl_tab()
    elif tab == "tab-pl-analysis":
        return render_pl_analysis_tab()
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
    fig = get_gamma_exposure_surface_data()
    return fig


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
# Tab 3: Trade Recommendations
# =====================
def render_trade_recommendations_tab():
    return html.Div(
        children=[
            html.H3("Trade Recommendations"),
            dcc.Dropdown(
                id="trade-recommendation-status",
                options=[
                    {"label": "Pending", "value": "pending"},
                    {"label": "Active", "value": "active"},
                    {"label": "Closed", "value": "closed"},
                ],
                placeholder="Select Trade Status",
            ),
            html.Div(id="trade-recommendation-table"),
        ]
    )


@callback(
    Output("trade-recommendation-table", "children"), Input("trade-recommendation-status", "value")
)
def update_trade_recommendation_table(status):
    trades_df = get_trade_recommendations(status)
    if trades_df.empty:
        return "No trade recommendations found."
    return trades_df.to_dict("records")


# =====================
# Tab 4: Live PnL Monitoring
# =====================
def render_live_pnl_tab():
    return html.Div(
        children=[
            html.H3("Live PnL Monitoring"),
            dcc.Interval(id="pnl-update-interval", interval=300000, n_intervals=0),
            html.Div(id="pnl-table"),
        ]
    )


@callback(Output("pnl-table", "children"), Input("pnl-update-interval", "n_intervals"))
def update_live_pnl_table(n_intervals):
    pnl_df = get_live_pnl_data()
    if pnl_df.empty:
        return "No open trades found."
    return pnl_df.to_dict("records")


# =====================
# Tab 5: P/L Analysis & Projections
# =====================
def render_pl_analysis_tab():
    return html.Div(
        children=[
            html.H3("P/L Analysis & Projections"),
            dcc.Dropdown(id="pl-trade-id-dropdown", placeholder="Select Trade ID"),
            html.Div(id="pl-analysis-content"),
            dcc.Graph(id="pl-projection-chart"),
        ]
    )


@callback(Output("pl-analysis-content", "children"), Input("pl-trade-id-dropdown", "value"))
def update_pl_analysis(trade_id):
    analysis_data = get_trade_pl_analysis(trade_id)
    if analysis_data.empty:
        return "No P/L analysis data found."
    data = analysis_data.iloc[0]
    return html.Div(
        [
            html.P(f"Max Profit: {data['max_profit']}"),
            html.P(f"Max Loss: {data['max_loss']}"),
            html.P(f"Breakeven: {data['breakeven_lower']} - {data['breakeven_upper']}"),
            html.P(f"Delta: {data['delta']}"),
            html.P(f"Theta: {data['theta']}"),
        ]
    )


@callback(Output("pl-projection-chart", "figure"), Input("pl-trade-id-dropdown", "value"))
def update_pl_projection_chart(trade_id):
    projections = get_trade_pl_projections(trade_id)
    if projections.empty:
        return {}
    return {
        "data": [{"x": projections["timestamp"], "y": projections["pnl"], "type": "line"}],
        "layout": {"title": f"P/L Projections for Trade {trade_id}"},
    }


@callback(Output("gamma-exposure-chart", "figure"), Input("gamma-expiry-dropdown", "value"))
def update_gamma_exposure_chart(expiration_date):
    if not expiration_date:
        return {
            "layout": {
                "title": "Please select an expiration date",
                "xaxis": {"title": "Strike Price"},
                "yaxis": {"title": "Net Gamma Exposure"},
            }
        }

    df, spot_price = get_gamma_exposure_for_expiry(expiration_date)
    if df.empty:
        return {
            "layout": {
                "title": f"No data for {expiration_date}",
                "xaxis": {"title": "Strike Price"},
                "yaxis": {"title": "Net Gamma Exposure"},
            }
        }

    # Define the figure
    fig = Figure()

    # Positive GEX - Blue
    fig.add_trace(
        Bar(
            x=df[df["net_gamma_exposure"] >= 0]["strike"],
            y=df[df["net_gamma_exposure"] >= 0]["net_gamma_exposure"],
            name="Positive GEX",
            marker_color="blue",
        )
    )

    # Negative GEX - Red
    fig.add_trace(
        Bar(
            x=df[df["net_gamma_exposure"] < 0]["strike"],
            y=df[df["net_gamma_exposure"] < 0]["net_gamma_exposure"],
            name="Negative GEX",
            marker_color="red",
        )
    )

    # Spot Price Line
    if spot_price:
        fig.add_shape(
            type="line",
            x0=spot_price,
            x1=spot_price,
            y0=df["net_gamma_exposure"].min(),
            y1=df["net_gamma_exposure"].max(),
            line=dict(color="black", dash="dash"),
        )

    # Zero Line
    fig.add_shape(
        type="line",
        x0=df["strike"].min(),
        x1=df["strike"].max(),
        y0=0,
        y1=0,
        line=dict(color="gray", dash="dot"),
    )

    # Update layout
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
