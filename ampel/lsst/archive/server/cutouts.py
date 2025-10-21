import io

import matplotlib.pyplot as plt
import numpy as np
from astropy.io import fits
from astropy.wcs import WCS
from matplotlib.colors import Normalize
from matplotlib.figure import Figure
from scipy import ndimage


def strip_extra_hdus(cutout_data: bytes) -> bytes:
    """
    Remove all HDUs except the primary from a cutout FITS file
    """
    with (
        fits.open(io.BytesIO(cutout_data), lazy_load_hdus=True) as hdus,
        io.BytesIO() as buf,
    ):
        hdus[:1].writeto(buf)
        return buf.getvalue()


def get_image_north_up_east_left(cutout_data: bytes) -> np.ndarray:
    """
    Rotate image cutout so that North is up and East is left
    """
    hdu = fits.open(io.BytesIO(cutout_data))[0]
    data = hdu.data
    rubinWCS = WCS(hdu.header)

    # Calculate rotation angle from original WCS
    cd = rubinWCS.pixel_scale_matrix
    # Adjust angle to rotate image to North up, East left
    theta = np.arctan2(cd[0, 1], cd[0, 0]) - np.pi / 2

    # Get reference pixel and image dimensions
    crpix1, crpix2 = rubinWCS.wcs.crpix
    ny, nx = data.shape

    # Create coordinate arrays
    y, x = np.ogrid[:ny, :nx]

    # Shift to make rotation point the origin
    x = x - crpix1
    y = y - crpix2

    # Rotate coordinates
    x_rot = x * np.cos(theta) - y * np.sin(theta)
    y_rot = x * np.sin(theta) + y * np.cos(theta)

    # Shift back and interpolate
    x_rot = x_rot + crpix1
    y_rot = y_rot + crpix2
    return ndimage.map_coordinates(data, [y_rot, x_rot])


def fits_to_png(cutout_data: bytes) -> bytes:
    """
    Render FITS as PNG
    """
    img = get_image_north_up_east_left(cutout_data)
    mask = np.isfinite(img)

    fig = Figure(figsize=(1, 1))
    try:
        ax = fig.add_axes((0.0, 0.0, 1.0, 1.0))
        ax.set_axis_off()
        ax.imshow(
            img,
            # clip pixel values below the median
            norm=Normalize(*np.percentile(img[mask], [0.5, 99.5])),
            aspect="auto",
            origin="lower",
        )

        with io.BytesIO() as buf:
            fig.savefig(buf, dpi=img.shape[0])
            return buf.getvalue()
    finally:
        fig.clear()
        plt.close(fig)
