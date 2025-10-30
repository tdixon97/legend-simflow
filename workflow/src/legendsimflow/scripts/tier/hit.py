# Copyright (C) 2025 Luigi Pertoldi <gipert@pm.me>,
#
# This program is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
from __future__ import annotations

import lgdo
import pyg4ometry
import pygeomtools
import reboost.spms
from lgdo import lh5
from lgdo.lh5 import LH5Iterator


def get_sensvols(geom, det_type: str | None = None) -> list[str]:
    sensvols = pygeomtools.detectors.get_all_sensvols(geom)
    if det_type is not None:
        return [k for k, v in sensvols.items() if v.detector_type == det_type]
    return list(sensvols.keys())


stp_file = snakemake.input.stp_file  # noqa: F821
hit_file = snakemake.output[0]  # noqa: F821
optmap_lar_file = snakemake.input.optmap_lar  # noqa: F821
gdml_file = snakemake.input.geom  # noqa: F821

# load the geometry
geom = pyg4ometry.gdml.Reader(gdml_file).getRegistry()
sensvols = pygeomtools.detectors.get_all_sensvols(geom)

lh5.write(lgdo.Struct(), "hit", hit_file, wo_mode="write_safe")

for det_name, meta in sensvols.items():
    if f"stp/{det_name}" not in lh5.ls(stp_file, "*/*"):
        continue

    iterator = LH5Iterator(stp_file, f"stp/{det_name}", buffer_len=100_000)

    if meta.detector_type == "scintillator" and det_name == "lar":
        for _chunk in iterator:
            chunk = _chunk.view_as("ak")

            _scint_ph = reboost.spms.pe.emitted_scintillation_photons(
                chunk.edep, chunk.particle, "lar"
            )
            for sipm in get_sensvols(geom, "optical"):
                sipm_uid = sensvols[sipm].uid

                optmap = reboost.spms.pe.load_optmap(optmap_lar_file, sipm_uid)

                photoelectrons = reboost.spms.pe.detected_photoelectrons(
                    _scint_ph,
                    chunk.particle,
                    chunk.time,
                    chunk.xloc,
                    chunk.yloc,
                    chunk.zloc,
                    optmap,
                    "lar",
                    sipm_uid,
                )

                out_table = lgdo.Table(size=len(chunk))
                out_table.add_field("t0", photoelectrons)

                lh5.write(
                    out_table,
                    f"hit/{sipm}",
                    hit_file,
                    wo_mode="append_column"
                    if iterator.current_i_entry == 0
                    else "append",
                )
