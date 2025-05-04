# =====================
# dashboard/app.py
# Dash app that visualizes SPX gamma exposure by strike, grouped by expiration
# =====================
import os

import dash
import plotly.graph_objs as go
from dash import Input, Output, dcc, html
from utils import get_available_expirations, get_gamma_exposure_df

app = dash.Dash(__name__)
app.title = "SPX Gamma Exposure"

app.layout = html.Div(
    [
        html.H1("SPX Gamma Exposure by Strike", style={"textAlign": "center"}),
        html.Div(
            [
                html.Label("Expiration Date:"),
                dcc.Dropdown(
                    id="expiration-dropdown",
                    options=[{"label": e, "value": e} for e in get_available_expirations()],
                    value=get_available_expirations()[0],
                    clearable=False,
                ),
            ],
            style={"width": "300px", "margin": "auto"},
        ),
        dcc.Graph(id="gex-chart"),
        dcc.Interval(id="interval", interval=15 * 60 * 1000, n_intervals=0),
    ]
)


@app.callback(
    Output("gex-chart", "figure"),
    [Input("interval", "n_intervals"), Input("expiration-dropdown", "value")],
)
def update_chart(_, expiration):
    df, current_price = get_gamma_exposure_df(expiration=expiration)
    if df.empty or current_price is None:
        return go.Figure().update_layout(title="No data available")

    df["distance"] = abs(df["strike"] - current_price)
    df = df.sort_values("distance").head(150)
    colors = ["green" if gex >= 0 else "red" for gex in df["gamma_exposure"]]

    fig = go.Figure(
        data=[
            go.Bar(
                x=df["strike"], y=df["gamma_exposure"], marker_color=colors, name="Gamma Exposure"
            ),
            go.Scatter(
                x=[current_price],
                y=[0],
                mode="markers+text",
                name="SPX Price",
                text=[f"SPX {current_price:.2f}"],
                textposition="top center",
                marker=dict(color="blue", size=12, symbol="line-ns-open"),
            ),
        ]
    )
    fig.update_layout(
        xaxis_title="Strike Price",
        yaxis_title="Net Gamma Exposure",
        title=f"SPX GEX for {expiration}",
        template="plotly_white",
        margin=dict(l=40, r=40, t=50, b=40),
    )
    return fig


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
