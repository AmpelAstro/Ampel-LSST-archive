from functools import lru_cache

from colorspacious import cspace_converter
from plotly.colors import validate_colors
from plotly.validator_cache import ValidatorCache


@lru_cache(maxsize=32)
def desaturate(name: str) -> list[tuple[float, str]]:
    """
    Create a desaturated version of the given colormap. Only useful for
    perceptually uniform colormaps.

    Based on
    https://matplotlib.org/stable/users/explain/colors/colormaps.html#grayscale-conversion
    """
    cmap = ValidatorCache.get_validator("heatmap", "colorscale").validate_coerce(name)
    values, colors = zip(*cmap, strict=False)
    # get luminance values in CAM02-UCS space
    lab = cspace_converter("sRGB1", "CAM02-UCS")(
        validate_colors(colors, colortype="tuple")
    )
    # scale colors to gray based on luminance
    return list(
        zip(
            values,
            ["#" + hex(round(v))[2:] * 3 for v in (lab[::, 0] / 100 * 256)],
            strict=False,
        )
    )
