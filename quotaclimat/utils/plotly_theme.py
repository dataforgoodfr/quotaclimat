import plotly.express as px
import plotly.graph_objects as go

# defining theme
# CONFIG AND THEMES
COLOR_SEQUENCE = [
    "rgb(230, 50, 24)",
    "rgb(240, 73, 70)",
    "rgb(243, 127, 125)",
    "rgb(248, 182, 181)",
    "rgb(209, 220, 197)",
    "rgb(137, 168, 141)",
    "rgb(59, 111, 66)",
    "rgb(66, 66, 66)",
]

SMALL_SEQUENCE2 = [
    "rgb(230, 50, 24)",
    "rgb(59, 111, 66)",
]

WARMING_STRIPES_SEQUENCE = [
    "#08306b",
    "#08519c",
    "#2171b5",
    "#4292c6",
    "#6baed6",
    "#9ecae1",
    "#c6dbef",
    "#deebf7",
    "#fee0d2",
    "#fcbba1",
    "#fc9272",
    "#fb6a4a",
    "#ef3b2c",
    "#cb181d",
    "#a50f15",
    "#67000d",
]

COLOR_SEQUENCE = COLOR_SEQUENCE  # + px.colors.qualitative.Antique
COLOR_SEQUENCE = px.colors.qualitative.Prism
px.defaults.template = "plotly_white"
px.defaults.color_discrete_sequence = COLOR_SEQUENCE

THEME = go.layout.Template()
THEME.layout.treemapcolorway = COLOR_SEQUENCE
THEME.layout.sunburstcolorway = COLOR_SEQUENCE
THEME.layout.colorway = COLOR_SEQUENCE
THEME.layout.piecolorway = COLOR_SEQUENCE
THEME.layout.font = {"family": "Poppins"}


def get_theme():
    return THEME
