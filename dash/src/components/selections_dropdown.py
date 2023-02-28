from dash import Dash, html, dcc
from dash.dependencies import Input,Output
from ..data.loader import DataSchema
from . import ids
import pandas as pd


def render(app: Dash) -> html.Div:
    all_selections = ["South Korea","China","Canada"]

    @app.callback(
        Output(ids.SELECTIONS_DROPDOWN, "value"),
        Input(ids.SELECT_ALL_BUTTON, "n_clicks")
    )
    def select_all_selections(_: int) -> list[str]:
        return all_selections

    return html.Div(
        children=[
            html.H6("dropdown options"),
            dcc.Dropdown(
                id=ids.SELECTIONS_DROPDOWN,
                options=[{"label":selection, "value": selection} for selection in all_selections],
                value=all_selections,
                multi=True
            ),
            html.Button(
                className="dropdown-button",
                children=["Select All"],
                id=ids.SELECT_ALL_BUTTON
            )
        ]
    )

def render_category(app: Dash, data: pd.DataFrame) -> html.Div:
    # all_selections = ["South Korea","China","Canada"]
    all_category = data[DataSchema.CATEGORY].tolist()
    unique_category = sorted(set(all_category))

    @app.callback(
        Output(ids.CATEGORY_DROPDOWN, "value"),
        Input(ids.SELECT_ALL_CATEGORY, "n_clicks")
    )
    def select_all_category(_: int) -> list[str]:
        return unique_category

    return html.Div(
        children=[
            html.H6("Category"),
            dcc.Dropdown(
                id=ids.CATEGORY_DROPDOWN,
                options=[{"label":category, "value": category} for category in unique_category],
                value=unique_category,
                multi=True
            ),
            html.Button(
                className="dropdown-button",
                children=["Select All"],
                id=ids.SELECT_ALL_CATEGORY
            )
        ]
    )