# dashboard/main.py
# =====================
# Multi‑Strategy Dashboard with unified PnL/Legs table, toggleable details.
# =====================

import os
import sys
from pathlib import Path

# Enable absolute imports & load .env
sys.path.append(str(Path(__file__).resolve().parents[1]))
if not (os.getenv("RENDER") or os.getenv("RAILWAY_ENVIRONMENT")):
    from dotenv import load_dotenv

    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

from dash import MATCH, Dash, Input, Output, callback_context, dcc, html
from flask_caching import Cache
from plotly.graph_objects import Bar, Figure
from utils.bq_queries import (
    get_available_expirations,
    get_gamma_exposure_for_expiry,
    get_gamma_exposure_surface_data,
    get_legs_data,
    get_live_pnl_data,
    get_trade_pl_analysis,
    get_trade_recommendations,
)

# Init app
app = Dash(__name__, suppress_callback_exceptions=True)
app.title = "📊 Multi‑Strategy Trading Dashboard"
cache = Cache(app.server, config={"CACHE_TYPE": "SimpleCache"})


@cache.memoize(timeout=300)
def _expirations():
    return get_available_expirations()[::-1]


@cache.memoize(timeout=300)
def _trade_ids():
    return get_trade_recommendations(None)["trade_id"].tolist()


# -- App layout ---------------------------------------------------------------
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
                dcc.Tab(label="Trade Recommendations", value="tab-trades"),
            ],
        ),
        html.Div(id="tabs-content"),
    ],
)


# -- Render tab content ------------------------------------------------------
@app.callback(Output("tabs-content", "children"), Input("tabs", "value"))
def _render_tab(tab):
    if tab == "tab-gamma-surface":
        return _gamma_surface_tab()
    if tab == "tab-gamma":
        return _gamma_analysis_tab()
    if tab == "tab-trades":
        return _trades_tab()
    return html.Div("Unknown tab selected")


def _gamma_surface_tab():
    return html.Div(
        [
            html.H3("3D Gamma Exposure Surface"),
            html.Button("Refresh", id="refresh-gamma-surface"),
            dcc.Graph(id="gamma-surface-chart"),
        ]
    )


@app.callback(Output("gamma-surface-chart", "figure"), Input("refresh-gamma-surface", "n_clicks"))
def _update_surface(_):
    return get_gamma_exposure_surface_data()


def _gamma_analysis_tab():
    return html.Div(
        [
            html.H3("Gamma Exposure Analysis"),
            dcc.Dropdown(
                id="gamma-expiry-dropdown",
                options=[{"label": d, "value": d} for d in _expirations()],
                placeholder="Select Expiration Date",
            ),
            dcc.Graph(id="gamma-exposure-chart"),
        ]
    )


@app.callback(Output("gamma-exposure-chart", "figure"), Input("gamma-expiry-dropdown", "value"))
def _update_gamma_chart(exp):
    if not exp:
        return {"layout": {"title": "Select an expiration date"}}
    df, spot = get_gamma_exposure_for_expiry(exp)
    if df.empty:
        return {"layout": {"title": f"No data for {exp}"}}
    fig = Figure()
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
    # spot line
    if spot is not None:
        fig.add_shape(
            type="line",
            x0=spot,
            x1=spot,
            y0=df.net_gamma_exposure.min(),
            y1=df.net_gamma_exposure.max(),
            line=dict(color="black", dash="dash"),
        )
    # zero line
    fig.add_shape(
        type="line",
        x0=df.strike.min(),
        x1=df.strike.max(),
        y0=0,
        y1=0,
        line=dict(color="gray", dash="dot"),
    )
    fig.update_layout(
        title=f"GEX on {exp}" + (f" | Spot≈{spot:.2f}" if spot else ""),
        xaxis_title="Strike",
        yaxis_title="Net Gamma Exposure",
        template="plotly_white",
        height=450,
    )
    return fig


def _trades_tab():
    return html.Div(
        [
            html.H3("Trade Recommendations", style={"textAlign": "center", "marginBottom": "1rem"}),
            dcc.Dropdown(
                id="trade-recommendation-status",
                options=[
                    {"label": s.capitalize(), "value": s} for s in ["pending", "active", "closed"]
                ],
                placeholder="Filter by status",
                style={"width": "300px", "margin": "auto 0 1rem 0"},
            ),
            html.Div(id="trade-recommendation-table"),
        ]
    )


@app.callback(
    Output("trade-recommendation-table", "children"),
    Input("trade-recommendation-status", "value"),
)
def _update_trade_table(status):
    df = get_trade_recommendations(status)
    if df.empty:
        return html.Div("No trades found.", style={"textAlign": "center", "marginTop": "2rem"})
    rows = []
    for _, r in df.iterrows():
        tid = r.trade_id
        rows.append(
            html.Div(
                [
                    html.Div(
                        [
                            html.Div(f"ID: {tid}", className="trade-id"),
                            html.Div(f"Strategy: {r.strategy_type}"),
                            html.Div(f"Status: {r.status}"),
                            html.Div(f"Entry: {r.entry_time}"),
                            html.Div(
                                f"{'Exit:' if r.status=='closed' else 'PnL:'} ${(r.exit_price if r.status=='closed' else r.pnl):.2f}"
                            ),
                            html.Button(
                                "Collapse" if False else "Expand",
                                id={"type": "expand-button", "index": tid},
                                n_clicks=0,
                                className="expand-button",
                            ),
                        ],
                        className="main-row",
                    ),
                    html.Div(
                        "",
                        id={"type": "collapse-content", "index": tid},
                        className="collapse-content",
                        style={"display": "none"},
                    ),
                ],
                className="trade-row",
            )
        )
    return html.Div(rows, className="trade-table")


@app.callback(
    Output({"type": "collapse-content", "index": MATCH}, "style"),
    Output({"type": "collapse-content", "index": MATCH}, "children"),
    Input({"type": "expand-button", "index": MATCH}, "n_clicks"),
)
def _toggle_details(n_clicks):
    open_ = (n_clicks or 0) % 2 == 1
    if not open_:
        return {"display": "none"}, []

    # Which trade?
    ctx = callback_context.triggered_id
    tid = ctx["index"]

    # 1) P/L analysis
    pl = get_trade_pl_analysis(tid).iloc[0]

    # 2) Legs & live/current
    legs = get_legs_data(tid)
    live = get_live_pnl_data(tid)

    # Join legs + live on leg_id
    merged = legs.merge(
        live[["leg_id", "current_price", "theoretical_pnl"]], on="leg_id", how="left"
    )

    # Build one table: Leg ID | Type | Dir | Strike | Entry | Curr/Exit | PnL
    header = html.Tr(
        [html.Th(c) for c in ["Leg ID", "Type", "Dir", "Strike", "Entry", "Curr/Exit", "PnL"]]
    )
    rows = []
    for _, l in merged.iterrows():
        # If trade closed, use exit_price & stored pnl; else live
        if l.status == "closed":
            price = l.exit_price
            pnl = l.pnl
        else:
            price = l.current_price
            pnl = l.theoretical_pnl
        rows.append(
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

    table = html.Table([header] + rows, className="legs-table")
    details = [
        html.H5("P/L Analysis"),
        html.P(f"Max Profit: ${pl.max_profit:.2f}"),
        html.P(f"Max Loss:  ${pl.max_loss:.2f}"),
        html.P(f"Breakeven: {pl.breakeven_lower:.2f} – {pl.breakeven_upper:.2f}"),
        html.P(f"Prob. Profit: {pl.probability_profit:.1f}%"),
        html.H5("Leg Details"),
        table,
    ]
    return {"display": "block"}, details


# -- Run ---------------------------------------------------------------------
if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
