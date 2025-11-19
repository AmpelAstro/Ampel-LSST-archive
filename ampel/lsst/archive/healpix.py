import astropy.units as u
import pyarrow as pa
import pyarrow.compute as pc
from astropy_healpix import lonlat_to_healpix

NSIDE = 1 << 16


def set_hpx(batch: pa.Table, nside=NSIDE, column_name="_hpx") -> pa.Table:
    hpx = pa.array(
        lonlat_to_healpix(
            pc.struct_field(batch["diaSource"], "ra") * u.deg,
            pc.struct_field(batch["diaSource"], "dec") * u.deg,
            nside=nside,
            order="nested",
        )
    )
    return batch.set_column(batch.schema.get_field_index(column_name), column_name, hpx)
