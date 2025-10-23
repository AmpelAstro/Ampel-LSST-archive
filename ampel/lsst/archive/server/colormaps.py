from functools import lru_cache

import matplotlib as mpl
from colorspacious import cspace_converter
from matplotlib.colors import ListedColormap


@lru_cache(maxsize=32)
def desaturate(name: str) -> ListedColormap:
    """
    Create a desaturated version of the given colormap. Only useful for
    perceptually uniform colormaps.

    Based on
    https://matplotlib.org/stable/users/explain/colors/colormaps.html#grayscale-conversion
    """
    cmap = mpl.colormaps[name]
    if not isinstance(cmap, ListedColormap):
        raise ValueError("Can only desaturate ListedColormaps")
    # get luminance values in CAM02-UCS space
    lab = cspace_converter("sRGB1", "CAM02-UCS")(cmap.colors)
    # scale colors to gray based on luminance
    colors = mpl.colormaps["binary_r"](lab[:, 0] / 100)[:, :-1]
    return ListedColormap(colors, f"{cmap.name}_desaturated")
