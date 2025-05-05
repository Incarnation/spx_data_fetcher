# app.py
import os
from datetime import datetime

import pandas as pd
from dash import Dash, Input, Output, dcc, html
from utils.bq_queries import get_available_expirations, get_gamma_exposure_for_expiry

# Initialize the Dash app
app = Dash(__name__)
app.title = "SPX Gamma Exposure Dashboard"

# Layout: Structured into a heading, dropdown selector, and graph
app.layout = html.Div(
    style={"fontFamily": "Arial", "maxWidth": "1000px", "margin": "auto", "padding": "20px"},
    children=[
        html.H1("📈 SPX Gamma Exposure", style={"textAlign": "center"}),
        html.P(
            "Select an expiration date to view gamma exposure by strike.",
            style={"textAlign": "center"},
        ),
        dcc.Dropdown(
            id="expiration-dropdown",
            options=[{"label": date, "value": date} for date in get_available_expirations()],
            placeholder="Select an expiration date",
            style={"marginBottom": "30px"},
        ),
        dcc.Graph(id="gex-chart"),
    ],
)


# Callback to update chart based on dropdown selection
@app.callback(
    Output("gex-chart", "figure"),
    Input("expiration-dropdown", "value"),
)
def update_chart(expiration_date):
    """
    Called when the dropdown value changes. It retrieves data from BigQuery
    and returns a Plotly figure showing gamma exposure by strike.
    """
    if not expiration_date:
        return {
            "layout": {
                "title": "Please select an expiration date",
                "xaxis": {"title": "Strike Price"},
                "yaxis": {"title": "Net Gamma Exposure"},
            }
        }

    # Retrieve the data from BigQuery
    df, spot_price = get_gamma_exposure_for_expiry(expiration_date)

    if df.empty:
        return {
            "layout": {
                "title": f"No data available for {expiration_date}",
                "xaxis": {"title": "Strike Price"},
                "yaxis": {"title": "Net Gamma Exposure"},
            }
        }

    # Create a Plotly line chart
    fig = {
        "data": [
            {
                "x": df["strike"],
                "y": df["net_gamma_exposure"],
                "type": "bar",
                "name": "GEX",
            }
        ],
        "layout": {
            "title": f"Gamma Exposure on {expiration_date} | Spot ≈ {spot_price:.2f}",
            "xaxis": {"title": "Strike Price"},
            "yaxis": {"title": "Net Gamma Exposure", "tickformat": ",.0f"},
            "shapes": [
                {
                    "type": "line",
                    "x0": spot_price,
                    "x1": spot_price,
                    "y0": df["net_gamma_exposure"].min(),
                    "y1": df["net_gamma_exposure"].max(),
                    "line": {"color": "red", "dash": "dash"},
                }
            ],
        },
    }

    return fig


# Run the app locally
if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
