# ruff: noqa: I002

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

import awkward as ak
import legenddataflowscripts as ldfs
import legenddataflowscripts.utils
import legendhpges
import lgdo
import pyg4ometry
import pygeomtools
import reboost.hpge.surface
import reboost.math.functions
import reboost.spms
from dbetto import AttrsDict
from legendmeta.police import validate_dict_schema
from lgdo import lh5
from lgdo.lh5 import LH5Iterator


def get_sensvols(geom, det_type: str | None = None) -> list[str]:
    sensvols = pygeomtools.detectors.get_all_sensvols(geom)
    if det_type is not None:
        return [k for k, v in sensvols.items() if v.detector_type == det_type]
    return list(sensvols.keys())


def write_chunk(iterator, table, objname):
    wo_mode = "append_column" if iterator.current_i_entry == 0 else "append"
    return lh5.write(
        table,
        objname,
        hit_file,
        wo_mode=wo_mode,
    )


stp_file = snakemake.input.stp_file  # noqa: F821
hit_file = snakemake.output[0]  # noqa: F821
optmap_lar_file = snakemake.input.optmap_lar  # noqa: F821
gdml_file = snakemake.input.geom  # noqa: F821
log_file = snakemake.log[0]  # noqa: F821
metadata = snakemake.config.metadata  # noqa: F821

# setup logging
log = ldfs.utils.build_log(metadata.simprod.config.logging, log_file)

# load the geometry and retrieve registered sensitive volumes
geom = pyg4ometry.gdml.Reader(gdml_file).getRegistry()
sensvols = pygeomtools.detectors.get_all_sensvols(geom)

# create the output file
lh5.write(lgdo.Struct(), "hit", hit_file, wo_mode="write_safe")

# loop over the registered sensitive volumes
for det_name, geom_meta in sensvols.items():
    if f"stp/{det_name}" not in lh5.ls(stp_file, "*/*"):
        msg = (
            f"detector {det_name} not found in {stp_file}. "
            "possibly because it was not read-out or there were no hits recorded"
        )
        log.warning(msg)
        continue

    # initialize the stp file iterator
    iterator = LH5Iterator(stp_file, f"stp/{det_name}", buffer_len=100_000)

    # process the HPGe output
    if geom_meta.detector_type == "germanium":
        msg = f"processing the {det_name} output table..."
        log.info(msg)

        det_meta = metadata.hardware.detectors.germanium.diodes[det_name]

        has_fccd_meta = validate_dict_schema(
            det_meta.characterization,
            {"combined_0vbb_analysis": {"fccd_in_mm": 0}},
            greedy=False,
            verbose=False,
        )

        if not has_fccd_meta:
            msg = f"{det_name} metadata does not seem to contain usable FCCD data, setting to 1 mm"
            log.warning(msg)
            fccd = 1
        else:
            fccd = det_meta.characterization.combined_0vbb_analysis.fccd_in_mm

        log.debug("creating an legendhpges.HPGe object")
        pyobj = legendhpges.make_hpge(
            geom_meta.metadata, registry=None, allow_cylindrical_asymmetry=False
        )

        det_loc = geom.physicalVolumeDict[det_name].position.eval()

        for _chunk in iterator:
            chunk = _chunk.view_as("ak")
            _distance_to_surf = AttrsDict()

            for surf in ("nplus", "pplus", "passive"):
                _distance_to_surf[surf] = reboost.hpge.surface.distance_to_surface(
                    chunk.xloc * 1000,  # mm
                    chunk.yloc * 1000,  # mm
                    chunk.zloc * 1000,  # mm
                    pyobj,
                    det_loc,
                    distances_precompute=chunk.dist_to_surf * 1000,
                    precompute_cutoff=(fccd + 1),
                    surface_type="nplus",
                )

            _activeness = reboost.math.functions.piecewise_linear_activeness(
                _distance_to_surf.nplus,
                fccd=fccd,
                dlf=0.2,
            )

            energy = ak.sum(chunk.edep * _activeness, axis=-1)

            out_table = lgdo.Table(size=len(chunk))
            out_table.add_field("energy", lgdo.Array(energy, attrs={"units": "keV"}))
            write_chunk(iterator, out_table, f"hit/{det_name}")

    # process the scintillator output
    if geom_meta.detector_type == "scintillator" and det_name == "lar":
        log.info("processing the 'lar' scintillator table...")

        for _chunk in iterator:
            chunk = _chunk.view_as("ak")

            _scint_ph = reboost.spms.pe.emitted_scintillation_photons(
                chunk.edep, chunk.particle, "lar"
            )
            for sipm in get_sensvols(geom, "optical"):
                sipm_uid = sensvols[sipm].uid

                msg = f"applying optical map for SiPM {sipm} (uid={sipm_uid})"
                log.debug(msg)

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
                    map_scaling=0.1,
                )

                out_table = lgdo.Table(size=len(chunk))
                out_table.add_field("t0", photoelectrons)
                write_chunk(iterator, out_table, f"hit/{sipm}")
