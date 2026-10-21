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
import logging
import re
from collections.abc import Mapping

import awkward as ak
import hist
import legenddataflowscripts as ldfs
import legenddataflowscripts.utils
import lh5
import numpy as np
from lgdo import Histogram, Scalar, Struct
from snakemake_argparse_bridge import snakemake_compatible

from legendsimflow import nersc, utils
from legendsimflow.metadata import get_tier_settings
from legendsimflow.pdf import (
    aoe_categories,
    fccd_by_uid,
    fccd_categories,
    format_pdf_struct,
    lar_categories,
)
from legendsimflow.scripts import log_script_invocation


def _detector_group_uids(
    groups: Mapping[str, str] | None,
    name_to_uid: Mapping[str, int],
    log: logging.Logger,
) -> dict[str, np.ndarray]:
    """Return the UIDs of the detectors in each group, plus the implicit ``all`` group."""
    groups = dict(groups or {})
    if groups.get("all", ".*") != ".*":
        log.warning(
            "detector_groups sets 'all' to %r; it will be overridden by the implicit all-detector group '.*'",
            groups["all"],
        )
    groups["all"] = ".*"

    group_uids = {}
    for name, pattern in groups.items():
        rgx = re.compile(pattern)
        matched = sorted(uid for n, uid in name_to_uid.items() if rgx.fullmatch(n))
        if not matched:
            log.warning("detector group %r matches no detectors", name)
        group_uids[name] = np.asarray(matched, dtype=np.int64)
    return group_uids


def _aoe_model(tier_hit_settings: Mapping) -> str | None:
    """Return the A/E model of the categories: the pulse-shape library if simulated, else the single template."""
    if tier_hit_settings.simulate_psd_with_psl:
        return "pulse_lib"
    if tier_hit_settings.simulate_psd:
        return "single_temp"
    return None


def _category_levels(
    has_spms_coinc: bool, aoe_model: str | None, fccd_thr: float | None
) -> list[dict[str, str]]:
    """Return the event categories (name -> description), one dict per level."""
    levels = []
    if has_spms_coinc:
        levels.append(
            {
                "is_spms_coincident": "light seen by the SiPMs (fails the LAr veto)",
                "not_spms_coincident": "no light seen by the SiPMs (passes the LAr veto)",
            }
        )
    if aoe_model is not None:
        aoe_level = {}
        # the single template has no high-side cut
        if aoe_model == "pulse_lib":
            aoe_level["is_high_aoe"] = "A/E above the high-side cut"
        aoe_level["is_ss_aoe"] = "single-site A/E (passes the A/E cuts)"
        aoe_level["is_low_aoe"] = "A/E below the low-side cut (multi-site)"
        aoe_level["is_invalid_aoe"] = "no valid or simulated A/E"
        levels.append(aoe_level)
    if fccd_thr is not None:
        levels.append(
            {
                "is_fccd_tagged": f"more than {fccd_thr} keV deposited in the FCCD layer",
                "not_fccd_tagged": f"at most {fccd_thr} keV deposited in the FCCD layer",
            }
        )
    return levels


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
    tier_evt_settings = get_tier_settings(config, "evt")
    tier_hit_settings = get_tier_settings(config, "hit")

    cvt_file = str(nersc.dvs_ro(config, args.cvt_file))
    pdf_file = args.pdf_file

    log = ldfs.utils.build_log(config.metadata.simprod.config.logging, args.log_file)
    log_script_invocation(log, "tier-pdf", parser, args)

    # the number of simulated primary events is written by remage at the root
    # of each stp file, forwarded through the evt tier and summed over all jobs
    # at the cvt tier.
    number_of_primaries = int(lh5.read("number_of_simulated_events", cvt_file).value)
    log.info(
        "... read number of simulated events from %s: %s", cvt_file, number_of_primaries
    )

    output = Struct()

    # without HPGe data (skip_hit) there is nothing to histogram
    if not tier_evt_settings.skip_hit:
        has_spms_coinc = not tier_evt_settings.skip_opt
        aoe_model = _aoe_model(tier_hit_settings)
        fccd_thr = tier_pdf_settings.fccd_tag_energy_thr_kev
        if fccd_thr is not None and not tier_hit_settings.store_surface_steps:
            msg = (
                "fccd_tag_energy_thr_kev needs the steps near the n+ surface, "
                "run the hit tier with store_surface_steps"
            )
            raise ValueError(msg)

        # name to UID map written at evt-tier build time
        detector_uids = lh5.read("detector_uids", cvt_file)
        name_to_uid = {n: int(detector_uids[n].value) for n in detector_uids}
        group_uids = _detector_group_uids(
            tier_pdf_settings.detector_groups, name_to_uid, log
        )
        if fccd_thr is not None:
            sorted_uids, fccd_mm = fccd_by_uid(
                config.metadata,
                name_to_uid,
                tier_hit_settings.surface_depth_in_mm,
            )

        # path in the output struct -> description. Each path is one
        # combination of categories, one per level; the paths do not overlap.
        levels = _category_levels(has_spms_coinc, aoe_model, fccd_thr)
        category_desc = {k: v for level in levels for k, v in level.items()}
        descriptions = {
            "/".join(("mul1", *combo)): "multiplicity 1"
            + "".join(f", {category_desc[c]}" for c in combo)
            for combo in itertools.product(*levels)
        }

        # 1 keV/bin over 0-6000 keV, split per detector group; boost-histogram
        # only provides Double (float64) storage
        histograms = {
            path: {g: hist.new.Reg(6000, 0, 6000).Double() for g in group_uids}
            for path in descriptions
        }
        # 2-D: (E_min, E_max) for multiplicity-2 events; not split by group
        mul2_hist = hist.new.Reg(6000, 0, 6000).Reg(6000, 0, 6000).Double()

        log.info("... beginning iteration over cvt file")

        # a single cvt file might be large, need to iterate
        buffer_len = tier_pdf_settings.buffer_len
        for chunk in lh5.LH5Iterator(cvt_file, "evt", buffer_len=buffer_len):
            data = chunk.view_as("ak")
            geds = data.geds

            # with store_surface_steps the evt tier also keeps the hits below
            # the energy threshold: drop them. The multiplicity is counted
            # here, not read from geds.multiplicity, so that it matches the
            # float32 energies.
            above_thr = geds.energy > tier_evt_settings.geds_energy_thr_kev
            multiplicity = ak.sum(above_thr, axis=-1)
            is_good = ak.all(geds.quality.is_good_channel[above_thr], axis=-1)

            energy_m2 = geds.energy[above_thr][(multiplicity == 2) & is_good]
            mul2_hist.fill(ak.min(energy_m2, axis=-1), ak.max(energy_m2, axis=-1))

            # multiplicity-1 events have exactly one hit above threshold
            is_m1 = (multiplicity == 1) & is_good
            evt_m1 = data[is_m1]
            hit_m1 = above_thr[is_m1]
            energy = ak.to_numpy(evt_m1.geds.energy[hit_m1][:, 0])
            uid = ak.to_numpy(evt_m1.geds.rawid[hit_m1][:, 0])

            # event masks of the categories of all levels
            in_category = {}
            if has_spms_coinc:
                in_category |= lar_categories(evt_m1)
            if aoe_model is not None:
                in_category |= aoe_categories(evt_m1, hit_m1, aoe_model)
            if fccd_thr is not None:
                fccd_of_hit = fccd_mm[np.searchsorted(sorted_uids, uid)]
                in_category |= fccd_categories(evt_m1, hit_m1, fccd_of_hit, fccd_thr)

            in_group = {g: np.isin(uid, uids) for g, uids in group_uids.items()}
            for path, per_group_hists in histograms.items():
                sel = np.ones(len(energy), dtype=bool)
                for c in path.split("/")[1:]:
                    sel &= in_category[c]
                for g, h in per_group_hists.items():
                    h.fill(energy[sel & in_group[g]])

        log.info("... convert histograms to lgdo")
        output = format_pdf_struct(histograms, descriptions)
        output.add_field(
            "mul2",
            Histogram(
                mul2_hist,
                attrs={
                    "description": "multiplicity-2 events: (E_low, E_high) for pairs of ON detectors"
                },
            ),
        )

    lh5.write(output, "pdf", pdf_file, wo_mode="w")
    lh5.write(
        Scalar(int(number_of_primaries)), "nr_sim_events", pdf_file, wo_mode="append"
    )


if __name__ == "__main__":
    main()
