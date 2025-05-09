from dash import Dash, dcc, html, page_container, Input, Output

external_scripts = [
  "https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4",
  "https://cdnjs.cloudflare.com/ajax/libs/chroma-js/2.1.0/chroma.min.js",
]

app = Dash(
  __name__,
  use_pages=True,
  title="Kåk",
  suppress_callback_exceptions=True,
  external_scripts=external_scripts,
  # prevent_initial_callbacks='initial_duplicate'
)

# print(page_registry)

app.layout = html.Div(
  id="app",
  children=[page_container, dcc.Location(id="location:app", refresh=False)],
)

app.clientside_callback(
  """
  function(theme) {
    document.body.dataset.theme = theme === undefined ? "light" : theme 
    return theme
  }
  """,
  Output("theme-toggle", "value"),
  Input("theme-toggle", "value"),
)

if __name__ == "__main__":
  app.run(
    debug=True,
    dev_tools_hot_reload=False,
  )  # , host='0.0.0.0', port=8080)
