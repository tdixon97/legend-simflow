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

    # a single cvt file might be large, need to iterate
    iterator = lh5.LH5Iterator(
        str(cvt_file),
        "evt",
        buffer_len=buffer_len,
    )

    # the number of simulated primary events is written by remage at the root
    # of each stp file, forwarded through the evt tier and summed over all jobs
    # at the cvt tier.
    number_of_primaries = int(
        lh5.read("number_of_simulated_events", str(cvt_file)).value
    )

    log.info(
        "... read number of simulated events from cvt file %s", number_of_primaries
    )

    # 1 keV/bin over 0-6000 keV; boost-histogram only provides Double (float64) storage
    h1 = hist.new.Reg(6000, 0, 6000).Double

    _geds_fields = set()
    if has_geds:
        _geds_fields = {
            k.removeprefix("evt/geds/") for k in lh5.ls(_cvt_file_str, "evt/geds/")
        }
        geds_energy_thr_kev = get_tier_settings(config, "evt").geds_energy_thr_kev

    psd_models = set()
    if "psd" in _geds_fields:
        psd_models = {
            k.removeprefix("evt/geds/psd/")
            for k in lh5.ls(_cvt_file_str, "evt/geds/psd/")
        }
    # the A/E categories come from the pulse-shape library when it was
    # simulated, otherwise from the single template (which has no high-side cut)
    aoe_model = None
    if "pulse_lib" in psd_models:
        aoe_model = "pulse_lib"
    elif "single_temp" in psd_models:
        aoe_model = "single_temp"

    # independent event categories (name -> description), one set per cut
    lar_categories = {}
    if has_spms_coinc:
        lar_categories = {
            "is_spms_coincident": "light seen by the SiPMs (fails the LAr veto)",
            "not_spms_coincident": "no light seen by the SiPMs (passes the LAr veto)",
        }
    aoe_categories = {}
    if aoe_model == "pulse_lib":
        aoe_categories["is_high_aoe"] = "A/E above the high-side cut"
    if aoe_model is not None:
        aoe_categories |= {
            "is_ss_aoe": "single-site A/E (passes the A/E cuts)",
            "is_low_aoe": "A/E below the low-side cut (multi-site)",
            "is_invalid_aoe": "no valid or simulated A/E",
        }

    # optional tag of the events with energy deposited in the FCCD layer
    fccd_thr = tier_pdf_settings.fccd_tag_energy_thr_kev
    fccd_categories = {}
    if has_geds and fccd_thr is not None:
        if "surface" not in _geds_fields:
            msg = (
                "fccd_tag_energy_thr_kev needs the steps near the n+ surface, "
                "run the hit tier with store_surface_steps"
            )
            raise ValueError(msg)
        fccd_categories = {
            "is_fccd_tagged": f"more than {fccd_thr} keV deposited in the FCCD layer",
            "not_fccd_tagged": f"at most {fccd_thr} keV deposited in the FCCD layer",
        }

        # FCCD of each detector, ordered by UID for the lookup of the hits
        surface_depth = get_tier_settings(config, "hit").surface_depth_in_mm
        fccd_uids = np.array(sorted(name_to_uid.values()), dtype=np.int64)
        uid_to_name = {uid: n for n, uid in name_to_uid.items()}
        fccd_of_uid = np.zeros(len(fccd_uids))
        for i, uid in enumerate(fccd_uids):
            name = uid_to_name[uid]
            fccd_of_uid[i] = get_sanitized_fccd(config.metadata, name)
            if fccd_of_uid[i] > surface_depth:
                log.warning(
                    "FCCD of %s (%s mm) exceeds surface_depth_in_mm, using %s mm",
                    name,
                    fccd_of_uid[i],
                    surface_depth,
                )
                fccd_of_uid[i] = surface_depth

    # path in the output struct -> description. Each path is one combination
    # of the categories, in the order mul1, LAr, A/E, FCCD; the paths do not
    # overlap.
    descriptions: dict[str, str] = {}
    if has_geds:
        category_desc = lar_categories | aoe_categories | fccd_categories
        levels = [c for c in (lar_categories, aoe_categories, fccd_categories) if c]
        for combo in itertools.product(*levels):
            descriptions["/".join(("mul1", *combo))] = "multiplicity 1" + "".join(
                f", {category_desc[c]}" for c in combo
            )

    # 1-D histograms are split per detector group; mul2 stays global.
    histograms = {path: {g: h1() for g in groups} for path in descriptions}

    mul2_hist = None
    if has_geds:
        # 2-D: (E_min, E_max) for multiplicity-2 events; not split by group
        mul2_hist = hist.new.Reg(6000, 0, 6000).Reg(6000, 0, 6000).Double()

    log.info("... beginning iteration over cvt file")

    for chunk in iterator:
        data = chunk.view_as("ak")

        if has_geds:
            # with store_surface_steps the evt tier keeps the hits below the
            # energy threshold: drop them from every per-hit field used here
            above_thr = data.geds.energy > geds_energy_thr_kev
            energy = data.geds.energy[above_thr]

            # awkward 2.x has no ak.isin: build per-group masks via flatten / np.isin / unflatten.
            counts = ak.num(energy)
            flat_uid = ak.flatten(data.geds.rawid[above_thr])
            det_in_group = {
                g: ak.unflatten(np.isin(flat_uid, uids), counts)
                for g, uids in group_uids.items()
            }

            good_channel_mask = ak.all(
                data.geds.quality.is_good_channel[above_thr], axis=-1
            )

            m1_event_mask = (data.geds.multiplicity == 1) & good_channel_mask
            data_m1 = data[m1_event_mask]
            above_m1 = above_thr[m1_event_mask]
            energy_m1 = energy[m1_event_mask]
            det_m1 = {g: m[m1_event_mask] for g, m in det_in_group.items()}

            # event masks of the categories
            in_category = {}
            if has_spms_coinc:
                in_category["is_spms_coincident"] = data_m1.coincident.spms
                in_category["not_spms_coincident"] = ~data_m1.coincident.spms
            if aoe_model is not None:
                psd = data_m1.geds.psd
                aoe = psd[aoe_model]
                valid = ak.all(psd.is_good[above_m1] & aoe.has_aoe[above_m1], axis=-1)
                single_site = ak.all(aoe.is_single_site[above_m1], axis=-1)
                high = ak.zeros_like(valid)
                if aoe_model == "pulse_lib":
                    high = valid & ak.all(aoe.is_high_aoe[above_m1], axis=-1)
                    in_category["is_high_aoe"] = high
                in_category["is_ss_aoe"] = valid & ~high & single_site
                in_category["is_low_aoe"] = valid & ~high & ~single_site
                in_category["is_invalid_aoe"] = ~valid
            if fccd_categories:
                surface = data_m1.geds.surface
                fccd = ak.unflatten(
                    fccd_of_uid[np.searchsorted(fccd_uids, flat_uid)], counts
                )[m1_event_mask]
                distance = surface.distance_to_nplus[above_m1]
                edep_fccd = ak.sum(surface.edep[above_m1][distance < fccd], axis=-1)
                tagged = ak.any(edep_fccd > fccd_thr, axis=-1)
                in_category["is_fccd_tagged"] = tagged
                in_category["not_fccd_tagged"] = ~tagged

            for path, per_group_hists in histograms.items():
                sel = np.ones(len(data_m1), dtype=bool)
                for c in path.split("/")[1:]:
                    sel = sel & in_category[c]
                for g, h in per_group_hists.items():
                    h.fill(ak.flatten(energy_m1[sel][det_m1[g][sel]]))

            energy_m2 = energy[(data.geds.multiplicity == 2) & good_channel_mask]
            assert mul2_hist is not None
            mul2_hist.fill(ak.min(energy_m2, axis=-1), ak.max(energy_m2, axis=-1))

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
    if mul2_hist is not None:
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
