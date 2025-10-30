# Copyright (C) 2023 Luigi Pertoldi <gipert@pm.me>
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

from dbetto import AttrsDict
from legendmeta import LegendMetadata
from legendmeta.police import validate_dict_schema

from . import patterns, utils
from .exceptions import SimflowConfigError
from .utils import get_simconfig


def get_simid_njobs(config, metadata, tier, simid):
    """Returns the number of macros that will be generated for a given `tier`
    and `simid`."""
    if tier not in ("ver", "stp"):
        tier = "stp"

    if "benchmark" in config and config.benchmark.get("enabled", False):
        return 1

    sconfig = get_simconfig(config, metadata, tier, simid=simid)

    if "vertices" in sconfig and "number_of_jobs" not in sconfig:
        return len(gen_list_of_simid_outputs(config, metadata, "ver", sconfig.vertices))
    if "number_of_jobs" in sconfig:
        return sconfig.number_of_jobs
    return get_simconfig(config, metadata, tier, simid=simid, field="number_of_jobs")


def gen_list_of_simid_inputs(config, metadata, tier, simid):
    """Generates the full list of input files for a `tier` and `simid`."""
    n_jobs = get_simid_njobs(config, metadata, tier, simid)
    return patterns.input_simid_filenames(config, n_jobs, tier=tier, simid=simid)


def gen_list_of_simid_outputs(config, metadata, tier, simid, max_files=None):
    """Generates the full list of output files for a `simid`."""
    n_jobs = get_simid_njobs(config, metadata, tier, simid)
    if max_files is not None:
        n_jobs = min(n_jobs, max_files)
    return patterns.output_simid_filenames(config, n_jobs, tier=tier, simid=simid)


def gen_list_of_plots_outputs(config, tier, simid):
    if tier == "stp":
        return [
            patterns.plots_filepath(config, tier=tier, simid=simid)
            + "/event-vertices-tier_stp.png"
        ]
    return []


# simid independent stuff


def collect_simconfigs(config, metadata, tiers):
    cfgs = []
    for tier in tiers:
        for sid in get_simconfig(config, metadata, tier):
            cfgs.append((tier, sid))

    return cfgs


def gen_list_of_all_simids(config, metadata, tier):
    if tier not in ("ver", "stp"):
        tier = "stp"

    return get_simconfig(config, metadata, tier).keys()


def gen_list_of_all_simid_outputs(config, metadata, tier):
    mlist = []
    slist = gen_list_of_all_simids(config, metadata, tier)
    for simid in slist:
        mlist += gen_list_of_simid_outputs(config, metadata, tier, simid)

    return mlist


def gen_list_of_all_plots_outputs(config, metadata, tier):
    mlist = []
    for simid in gen_list_of_all_simids(config, metadata, tier):
        mlist += gen_list_of_plots_outputs(config, metadata, tier, simid)

    return mlist


# drift time maps


def crystal_meta(metadata: LegendMetadata, diode_meta: AttrsDict) -> AttrsDict:
    """Get the crystal metadata starting from the diode metadata."""
    ids = {"bege": "B", "coax": "C", "ppc": "P", "icpc": "V"}
    crystal_name = (
        ids[diode_meta.type]
        + format(diode_meta.production.order, "02d")
        + diode_meta.production.crystal
    )
    crystal_db = metadata.hardware.detectors.germanium.crystals
    if crystal_name in crystal_db:
        return crystal_db[crystal_name]
    return None


def start_key(metadata: LegendMetadata, runid: str) -> str:
    """Get the start key for a runid."""
    _, period, run, datatype = runid.split("-")
    return metadata.datasets.runinfo[period][run][datatype].start_key


def gen_list_of_hpges_valid_for_dtmap(
    metadata: LegendMetadata, runid: str
) -> list[str]:
    """Make a list of HPGe detector for which we want to generate a drift time map.

    It generates the list of deployed detectors in `runid` via the LEGEND
    channelmap, then checks if in the crystal metadata there's all the
    information required to generate a drift time map.
    """
    chmap = metadata.hardware.configuration.channelmaps.on(start_key(metadata, runid))

    hpges = []
    for _, hpge in chmap.group("system").geds.items():
        m = crystal_meta(
            metadata, metadata.hardware.detectors.germanium.diodes[hpge.name]
        )

        if m is not None:
            schema = {"impurity_curve": {"parameters": [], "corrections": {"scale": 0}}}

            if validate_dict_schema(
                m, schema, greedy=False, typecheck=False, verbose=False
            ):
                hpges.append(hpge.name)

    return hpges


def gen_list_of_dtmaps(
    config: AttrsDict, metadata: LegendMetadata, runid: str
) -> list[str]:
    """Generate the list of HPGe drift time map files for a `runid`."""
    hpges = gen_list_of_hpges_valid_for_dtmap(metadata, runid)
    return [
        patterns.output_dtmap_filename(config, hpge_detector=hpge, runid=runid)
        for hpge in hpges
    ]


def gen_list_of_merged_dtmaps(config: AttrsDict) -> list[str]:
    r"""Generate the list of (merged) HPGe drift time map files for all requested `runid`\ s."""
    runlist = utils.get_some_list(config.runlist)
    return [
        patterns.output_dtmap_merged_filename(config, runid=runid) for runid in runlist
    ]


# evt tier


def gen_list_of_tier_evt_outputs(config, simid):
    runlist = utils.get_some_list(config.runlist)

    mlist = []
    for runid in runlist:
        mlist += [patterns.output_evt_filename(config, simid=simid, runid=runid)]

    return mlist


def gen_list_of_all_tier_evt_outputs(config):
    mlist = []
    slist = gen_list_of_all_simids(config, tier="stp")
    for sid in slist:
        mlist += gen_list_of_tier_evt_outputs(config, simid=sid)

    return mlist


# pdf tier


def gen_list_of_tier_pdf_outputs(config, simid):
    return [patterns.output_pdf_filename(config, simid=simid)]


def gen_list_of_all_tier_pdf_outputs(config):
    mlist = []
    slist = gen_list_of_all_simids(config, tier="stp")
    for simid in slist:
        mlist += gen_list_of_tier_pdf_outputs(config, simid=simid)
    return mlist


def process_simlist(config, metadata, simlist=None):
    if simlist is None:
        simlist = config.simlist

    # if it's a list, every item is a simid
    # otherwise, interpret as comma-separated list
    if not isinstance(simlist, list):
        simlist = simlist.split(",")

    mlist = []
    for line in simlist:
        # each line is in the format <tier>.<simid>
        if len(line.split(".")) != 2:
            msg = (
                "simflow-config.runlist",
                f"item '{line}' is not in the format <tier>.<simid>",
            )
            raise SimflowConfigError(*msg)

        tier = line.split(".")[0].strip()
        simid = line.split(".")[1].strip()

        # mlist += gen_list_of_plots_outputs(config, tier, simid)
        if tier in ("ver", "stp", "hit"):
            mlist += gen_list_of_simid_outputs(config, metadata, tier, simid)
        elif tier == "evt":
            mlist += gen_list_of_tier_evt_outputs(config, simid)
        elif tier == "pdf":
            mlist += gen_list_of_tier_pdf_outputs(config, simid)

    return mlist
