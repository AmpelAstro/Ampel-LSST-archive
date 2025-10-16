import io

import matplotlib.pyplot as plt
import numpy as np
from astropy.io import fits
from matplotlib.colors import Normalize
from matplotlib.figure import Figure


def fits_to_png(cutout_data: bytes) -> bytes:
    """
    Render FITS as PNG
    """
    img = np.flipud(fits.getdata(io.BytesIO(cutout_data)))
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
