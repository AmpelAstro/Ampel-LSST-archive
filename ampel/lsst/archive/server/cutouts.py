import io
from typing import TypedDict

import matplotlib.pyplot as plt
import numpy as np
from astropy.io import fits
from astropy.modeling import fitting, models
from astropy.wcs import WCS
from matplotlib.colors import Normalize
from matplotlib.figure import Figure
from matplotlib.offsetbox import AnchoredText
from matplotlib.patches import Ellipse
from mpl_toolkits.axes_grid1.anchored_artists import AnchoredSizeBar
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


class EllipseParams(TypedDict):
    xy: tuple[float, float]
    width: float
    height: float
    angle: float


# factor to convert sigma to FWHM ~ 2.355
FWHM = 2 * np.sqrt(2 * np.log(2))


def get_halfmax_ellipse(flux: fits.PrimaryHDU, psf: fits.ImageHDU) -> EllipseParams:
    """
    Extract PSF from cutout HDUList. Returns parameters for
    matplotlib.patches.Ellipse representing the half-maximum contour.
    """
    psf_data = psf.data

    # guess mean from peak position
    y0, x0 = np.unravel_index(np.argmax(psf_data), psf_data.shape)
    # guess standard deviation from peak amplitude, assuming it's a gaussian
    s0 = 1 / np.sqrt(2 * np.pi * psf_data.max())
    a0 = psf_data.max()
    p_init = models.Gaussian2D(
        amplitude=a0,
        x_mean=x0,
        y_mean=y0,
        x_stddev=s0,
        y_stddev=s0,
        theta=0,
    )
    y, x = np.mgrid[: psf_data.shape[0], : psf_data.shape[1]]
    fit_p = fitting.TRFLSQFitter()(p_init, x, y, psf_data)
    # find offset of center from cutout center
    dx = fit_p.x_mean.value - psf_data.shape[0] // 2
    dy = fit_p.y_mean.value - psf_data.shape[1] // 2

    # reference coordinates of the cutout
    # NB: pixel coordinates are 1-indexed in FITS
    crpix = WCS(flux.header).wcs.crpix - 1

    return {
        # center on reference point of the cutout
        "xy": (crpix[0] + dx, crpix[1] + dy),
        "width": (FWHM * fit_p.x_stddev.value).tolist(),
        "height": (FWHM * fit_p.y_stddev.value).tolist(),
        "angle": np.degrees(fit_p.theta.value).tolist(),
    }


def make_cutout_plots(cutouts: dict[str, bytes]):
    shrink = 1 / 1.2

    fig, axes = plt.subplots(
        1,
        len(cutouts),
        figsize=(5.0 * len(cutouts) * shrink, 5.0),
        gridspec_kw={"wspace": 0.05},
        layout="constrained",
    )
    show_scale_bar = True
    for (k, v), ax in zip(cutouts.items(), axes, strict=False):
        hdus = fits.open(io.BytesIO(v))
        img = hdus[0].data

        mask = np.isfinite(img)
        axes_image = ax.imshow(
            img,
            origin="lower",
            cmap="viridis",
            # clip pixel values below the median
            norm=Normalize(*np.percentile(img[mask], [0.5, 99.5])),
        )
        ax.set_axis_off()

        # add PSF half-maximum ellipse
        ellipse_params = get_halfmax_ellipse(hdus[0], hdus[2])
        ellipse = Ellipse(
            **ellipse_params, edgecolor="white", facecolor="none", lw=1, ls="--"
        )
        ax.add_patch(ellipse)

        # add scale bar
        # CDELT1 and CDELT2 are set to 1, but https://rubinobservatory.org/for-scientists/rubin-101/key-numbers says 0.2 arcsec/pixel
        if show_scale_bar:
            show_scale_bar = False
            ax.add_artist(
                AnchoredSizeBar(
                    ax.transData,
                    1 / 0.2,  # 1 arcsec in pixels
                    "1''",
                    "lower right",
                    pad=0.5,
                    frameon=False,
                    sep=4,
                    color="white",
                )
            )
        # label cutout type
        text = AnchoredText(k, loc="upper left", prop=dict(color="white"))
        text.patch.set_facecolor(axes_image.get_cmap()(0))
        text.patch.set_edgecolor("none")
        text.patch.set_boxstyle("round,pad=0.1,rounding_size=0.2")
        ax.add_artist(text)

        # add colorbar
        plt.colorbar(axes_image, orientation="horizontal", label="flux (nJy)", shrink=1)
    return fig


def render_cutout_plots(fits: dict[str, bytes]) -> str:
    fig = make_cutout_plots(fits)
    try:
        with io.BytesIO() as buf:
            fig.savefig(buf, format="svg")
            return buf.getvalue().decode()
    finally:
        fig.clear()
        plt.close(fig)


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
