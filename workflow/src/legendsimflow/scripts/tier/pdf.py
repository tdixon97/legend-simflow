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

import argparse
import itertools
import re

import awkward as ak
import hist
import legenddataflowscripts as ldfs
import legenddataflowscripts.utils
import lh5
import numpy as np
from lgdo import Histogram, Scalar, Struct
from snakemake_argparse_bridge import snakemake_compatible

from legendsimflow import nersc, utils
from legendsimflow.metadata import get_sanitized_fccd, get_tier_settings
from legendsimflow.scripts import log_script_invocation


def _one_hit(field: ak.Array, is_hit: ak.Array) -> ak.Array:
    """Return the field of the one selected hit of each event."""
    return field[is_hit][:, 0]


@snakemake_compatible(
    mapping={
        "cvt_file": "input.cvt_file",
        "pdf_file": "output[0]",
        "log_file": "log[0]",
        "simid": "wildcards.simid",
        "simflow_config": "config",
    }
)
def main() -> None:
    parser = argparse.ArgumentParser(description="Build the pdf tier.")
    parser.add_argument("--cvt-file", required=True, help="input cvt tier file")
    parser.add_argument("--pdf-file", required=True, help="output pdf tier file")
    parser.add_argument("--simid", required=True, help="simulation ID wildcard")
    parser.add_argument("--log-file", default=None, help="log file")
    parser.add_argument(
        "--simflow-config",
        "--config",
        dest="simflow_config",
        required=True,
        help="simflow config YAML path",
    )
    args = parser.parse_args()

    config = utils.init_simflow_context(args.simflow_config, workflow=None).config

    tier_pdf_settings = get_tier_settings(config, "pdf")
    buffer_len = tier_pdf_settings.buffer_len

    cvt_file = nersc.dvs_ro(config, args.cvt_file)
    pdf_file = args.pdf_file
    log_file = args.log_file

    log = ldfs.utils.build_log(config.metadata.simprod.config.logging, log_file)
    log_script_invocation(log, "tier-pdf", parser, args)

    log.info("... reading data from %s", cvt_file)

    # detect what data is available in the cvt file
    _cvt_file_str = str(cvt_file)
    _evt_groups = {k.removeprefix("evt/") for k in lh5.ls(_cvt_file_str, "evt/")}
    has_geds = "geds" in _evt_groups
    has_spms_coinc = "coincident" in _evt_groups and "spms" in {
        k.removeprefix("evt/coincident/")
        for k in lh5.ls(_cvt_file_str, "evt/coincident/")
    }

    # resolve detector-group regexes against the name to UID map written at
    # evt-tier build time; the implicit "all" group is always emitted.
    detector_uids = lh5.read("detector_uids", str(cvt_file))
    name_to_uid = {name: int(detector_uids[name].value) for name in detector_uids}

    groups: dict[str, str] = dict(tier_pdf_settings.detector_groups or {})
    if groups.get("all", ".*") != ".*":
        log.warning(
            "detector_groups sets 'all' to %r; it will be overridden by the implicit all-detector group '.*'",
            groups["all"],
        )
    groups["all"] = ".*"

    group_uids: dict[str, np.ndarray] = {}
    for gname, pat in groups.items():
        rgx = re.compile(pat)
        matched = sorted(uid for n, uid in name_to_uid.items() if rgx.fullmatch(n))
        # skip_hit upstream produces an empty detector_uids by design; warning would be spurious.
        if not matched and has_geds:
            log.warning("detector group %r matches no detectors", gname)
        group_uids[gname] = np.asarray(matched, dtype=np.int64)

    # a single cvt file might be large, need to iterate. Without HPGe data
    # there is nothing to fill.
    iterator = []
    if has_geds:
        iterator = lh5.LH5Iterator(str(cvt_file), "evt", buffer_len=buffer_len)
        geds_energy_thr_kev = get_tier_settings(config, "evt").geds_energy_thr_kev

    # the number of simulated primary events is written by remage at the root
    # of each stp file, forwarded through the evt tier and summed over all jobs
    # at the cvt tier.
    number_of_primaries = int(
        lh5.read("number_of_simulated_events", str(cvt_file)).value
    )

    log.info(
        "... read number of simulated events from cvt file %s", number_of_primaries
    )

    # independent event categories (name -> description), one dict per level.
    # A level is left out when its data is missing.
    levels: list[dict[str, str]] = []

    if has_spms_coinc:
        levels.append(
            {
                "is_spms_coincident": "light seen by the SiPMs (fails the LAr veto)",
                "not_spms_coincident": "no light seen by the SiPMs (passes the LAr veto)",
            }
        )

    # the A/E categories come from the pulse-shape library when it was
    # simulated, otherwise from the single template (which has no high-side
    # cut). Without simulated PSD there is no geds/psd and no A/E level.
    geds_fields = set(lh5.ls(_cvt_file_str, "evt/geds/")) if has_geds else set()
    psd_models = set()
    if "evt/geds/psd" in geds_fields:
        psd_models = set(lh5.ls(_cvt_file_str, "evt/geds/psd/"))
    aoe_model = None
    if "evt/geds/psd/pulse_lib" in psd_models:
        aoe_model = "pulse_lib"
    elif "evt/geds/psd/single_temp" in psd_models:
        aoe_model = "single_temp"
    if aoe_model is not None:
        aoe_level = {}
        if aoe_model == "pulse_lib":
            aoe_level["is_high_aoe"] = "A/E above the high-side cut"
        aoe_level["is_ss_aoe"] = "single-site A/E (passes the A/E cuts)"
        aoe_level["is_low_aoe"] = "A/E below the low-side cut (multi-site)"
        aoe_level["is_invalid_aoe"] = "no valid or simulated A/E"
        levels.append(aoe_level)

    fccd_thr = tier_pdf_settings.fccd_tag_energy_thr_kev
    if has_geds and fccd_thr is not None:
        if "evt/geds/surface" not in geds_fields:
            msg = (
                "fccd_tag_energy_thr_kev needs the steps near the n+ surface, "
                "run the hit tier with store_surface_steps"
            )
            raise ValueError(msg)
        levels.append(
            {
                "is_fccd_tagged": f"more than {fccd_thr} keV deposited in the FCCD layer",
                "not_fccd_tagged": f"at most {fccd_thr} keV deposited in the FCCD layer",
            }
        )

        # FCCD of each detector in mm, in the order of the sorted UIDs
        surface_depth = get_tier_settings(config, "hit").surface_depth_in_mm
        names = sorted(name_to_uid, key=name_to_uid.get)
        sorted_uids = np.array([name_to_uid[n] for n in names], dtype=np.int64)
        fccd_mm = np.zeros(len(names))
        for i, name in enumerate(names):
            fccd_mm[i] = get_sanitized_fccd(config.metadata, name)
            if fccd_mm[i] > surface_depth:
                log.warning(
                    "FCCD of %s (%s mm) exceeds surface_depth_in_mm, using %s mm",
                    name,
                    fccd_mm[i],
                    surface_depth,
                )
                fccd_mm[i] = surface_depth

    # path in the output struct -> description. Each path is one combination
    # of categories, one per level; the paths do not overlap.
    descriptions: dict[str, str] = {}
    if has_geds:
        category_desc = {k: v for level in levels for k, v in level.items()}
        for combo in itertools.product(*levels):
            descriptions["/".join(("mul1", *combo))] = "multiplicity 1" + "".join(
                f", {category_desc[c]}" for c in combo
            )

    # 1 keV/bin over 0-6000 keV, split per detector group; boost-histogram
    # only provides Double (float64) storage
    histograms = {
        path: {g: hist.new.Reg(6000, 0, 6000).Double() for g in groups}
        for path in descriptions
    }
    # 2-D: (E_min, E_max) for multiplicity-2 events; not split by group
    mul2_hist = hist.new.Reg(6000, 0, 6000).Reg(6000, 0, 6000).Double()

    log.info("... beginning iteration over cvt file")

    for chunk in iterator:
        data = chunk.view_as("ak")
        geds = data.geds

        # with store_surface_steps the evt tier also keeps the hits below the
        # energy threshold: drop them. The multiplicity is counted here, not
        # read from geds.multiplicity, so that it matches the float32 energies.
        above_thr = geds.energy > geds_energy_thr_kev
        multiplicity = ak.sum(above_thr, axis=-1)
        is_good = ak.all(geds.quality.is_good_channel[above_thr], axis=-1)

        energy_m2 = geds.energy[above_thr][(multiplicity == 2) & is_good]
        mul2_hist.fill(ak.min(energy_m2, axis=-1), ak.max(energy_m2, axis=-1))

        # multiplicity-1 events have exactly one hit above threshold: keep it,
        # so that every per-hit field has one entry per event
        is_m1 = (multiplicity == 1) & is_good
        evt_m1 = data[is_m1]
        hit_m1 = above_thr[is_m1]

        energy = ak.to_numpy(_one_hit(evt_m1.geds.energy, hit_m1))
        uid = ak.to_numpy(_one_hit(evt_m1.geds.rawid, hit_m1))

        in_category = {}
        if has_spms_coinc:
            spms = ak.to_numpy(evt_m1.coincident.spms)
            in_category["is_spms_coincident"] = spms
            in_category["not_spms_coincident"] = ~spms
        if aoe_model is not None:
            psd = evt_m1.geds.psd
            aoe = psd[aoe_model]
            valid = ak.to_numpy(
                _one_hit(psd.is_good, hit_m1) & _one_hit(aoe.has_aoe, hit_m1)
            )
            single_site = ak.to_numpy(_one_hit(aoe.is_single_site, hit_m1))
            high = np.zeros(len(valid), dtype=bool)
            if aoe_model == "pulse_lib":
                high = valid & ak.to_numpy(_one_hit(aoe.is_high_aoe, hit_m1))
                in_category["is_high_aoe"] = high
            in_category["is_ss_aoe"] = valid & ~high & single_site
            in_category["is_low_aoe"] = valid & ~high & ~single_site
            in_category["is_invalid_aoe"] = ~valid
        if fccd_thr is not None:
            # energy of the steps closer to the n+ surface than the FCCD
            surface = evt_m1.geds.surface
            fccd = fccd_mm[np.searchsorted(sorted_uids, uid)]
            in_fccd = _one_hit(surface.distance_to_nplus, hit_m1) < fccd
            edep_fccd = ak.sum(_one_hit(surface.edep, hit_m1)[in_fccd], axis=-1)
            tagged = ak.to_numpy(edep_fccd > fccd_thr)
            in_category["is_fccd_tagged"] = tagged
            in_category["not_fccd_tagged"] = ~tagged

        in_group = {g: np.isin(uid, uids) for g, uids in group_uids.items()}
        for path, per_group_hists in histograms.items():
            sel = np.ones(len(energy), dtype=bool)
            for c in path.split("/")[1:]:
                sel &= in_category[c]
            for g, h in per_group_hists.items():
                h.fill(energy[sel & in_group[g]])

    log.info("... convert histograms to lgdo")

    tree: dict = {}
    for path, per_group in histograms.items():
        node = tree
        for level in path.split("/"):
            node = node.setdefault(level, {})
        node.update(
            {
                g: Histogram(h, attrs={"description": descriptions[path]})
                for g, h in per_group.items()
            }
        )
    if has_geds:
        tree["mul2"] = Histogram(
            mul2_hist,
            attrs={
                "description": "multiplicity-2 events: (E_low, E_high) for pairs of ON detectors"
            },
        )

    def _to_struct(node: dict) -> Struct:
        return Struct(
            {k: _to_struct(v) if isinstance(v, dict) else v for k, v in node.items()}
        )

    output = _to_struct(tree)
    lh5.write(output, "pdf", pdf_file, wo_mode="w")
    lh5.write(
        Scalar(int(number_of_primaries)), "nr_sim_events", pdf_file, wo_mode="append"
    )


if __name__ == "__main__":
    main()
