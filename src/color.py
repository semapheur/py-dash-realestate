import colorsys


def rgba_to_hex(rgba: tuple[float, float, float, float]) -> str:
  r, g, b, a = rgba
  r_hex = hex(int(r * 255))[2:].zfill(2)
  g_hex = hex(int(g * 255))[2:].zfill(2)
  b_hex = hex(int(b * 255))[2:].zfill(2)
  a_hex = hex(int(a * 255))[2:].zfill(2)
  return f"#{r_hex}{g_hex}{b_hex}{a_hex}"


def viridis_color_scale(n):
  colors = []
  for i in range(n):
    h = i / (n - 1)  # normalize to [0, 1]
    r, g, b = colorsys.hsv_to_rgb(h, 1, 1)  # convert to RGB
    r, g, b = int(r * 255), int(g * 255), int(b * 255)  # convert to [0, 255]
    colors.append(f"rgb({r}, {g}, {b})")
  return colors
