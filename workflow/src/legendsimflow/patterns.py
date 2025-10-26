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

"""Prepare pattern strings to be used in Snakemake rules.

Extra keyword arguments are typically interpreted as variables to be
substituted in the returned (structure of) strings. They are passed to
:func:`snakemake.io.expand`.

Definitions:

- ``simid``: string identifier for the simulation run
- ``simjob``: one job of a simulation run (corresponds to one macro file and one output file)
- ``jobid``: zero-padded integer (i.e., a string) used to label a simulation job
"""

from __future__ import annotations

from pathlib import Path

from dbetto import AttrsDict
from snakemake.io import expand

from .utils import get_simconfig

FILETYPES = AttrsDict(
    {
        "input": {
            "ver": ".mac",
            "stp": ".mac",
            "hit": ".lh5",
            "evt": ".lh5",
            "pdf": ".lh5",
        },
        "output": {
            "ver": ".lh5",
            "stp": ".lh5",
            "hit": ".lh5",
            "evt": ".lh5",
            "pdf": ".lh5",
        },
    }
)


def simjob_rel_basename(**kwargs):
    """Formats a partial output path for a `simid` and `jobid`."""
    return expand("{simid}/{simid}_{jobid}", **kwargs, allow_missing=True)[0]


def log_filename(config, time, **kwargs):
    """Formats a log file path for a `simid` and `jobid`."""
    pat = str(
        Path(config.paths.log)
        / time
        / "{tier}"
        / (simjob_rel_basename() + "-tier_{tier}.log")
    )
    return expand(pat, **kwargs, allow_missing=True)[0]


def benchmark_filename(config, **kwargs):
    """Formats a benchmark file path for a `simid` and `jobid`."""
    pat = str(
        Path(config.paths.benchmarks)
        / "{tier}"
        / (simjob_rel_basename() + "-tier_{tier}.tsv")
    )
    return expand(pat, **kwargs, allow_missing=True)[0]


def plots_filepath(config, **kwargs):
    """Formats a benchmark file path for a `simid` and `jobid`."""
    pat = str(Path(config.paths.plots) / "{tier}" / "{simid}")
    return expand(pat, **kwargs, allow_missing=True)[0]


# ver, stp, hit tiers


def geom_filename(config):
    return Path(config.paths.geom) / f"{config['experiment']}-geom.gdml"


def geom_config(config):
    return Path(config.paths.config) / f"geom/{config['experiment']}-geom-config.yaml"


def geom_log_filename(config, time, **kwargs):
    pat = str(Path(config.paths.log) / time / f"geom/{config['experiment']}-geom.log")
    return expand(pat, **kwargs, allow_missing=True)[0]


def input_simjob_filename(config, **kwargs):
    """Returns the full path to the input file for a `simid`, `tier` and job index."""
    tier = kwargs.get("tier")

    if tier is None:
        msg = "the 'tier' argument is mandatory"
        raise RuntimeError(msg)

    fname = "{simid}" + f"-tier_{tier}" + FILETYPES["input"][tier]
    expr = str(Path(config.paths.macros) / f"{tier}" / fname)
    return expand(expr, **kwargs, allow_missing=True)[0]


def output_simjob_filename(config, **kwargs):
    """Returns the full path to the output file for a `simid`, `tier` and job index."""
    tier = kwargs.get("tier")

    if tier is None:
        msg = "the 'tier' argument is mandatory"
        raise RuntimeError(msg)

    fname = simjob_rel_basename() + f"-tier_{tier}" + FILETYPES["output"][tier]
    expr = str(Path(config.paths[f"tier_{tier}"]) / fname)
    return expand(expr, **kwargs, allow_missing=True)[0]


def output_simjob_regex(config, **kwargs):
    tier = kwargs.get("tier")

    if tier is None:
        msg = "the 'tier' argument is mandatory"
        raise RuntimeError(msg)

    fname = "*-tier_{tier}" + FILETYPES["output"][tier]
    expr = str(Path(config["paths"][f"tier_{tier}"]) / "{simid}" / fname)
    return expand(expr, **kwargs, allow_missing=True)[0]


def input_simid_filenames(config, n_macros, **kwargs):
    """Returns the full path to `n_macros` input files for a `simid`. Needed by
    script that generates all macros for a `simid`.
    """
    pat = input_simjob_filename(config, **kwargs)
    jobids = expand("{id:>04d}", id=list(range(n_macros)))
    return expand(pat, jobid=jobids, **kwargs, allow_missing=True)


def output_simid_filenames(config, n_macros, **kwargs):
    """Returns the full path to `n_macros` output files for a `simid`."""
    pat = output_simjob_filename(config, **kwargs)
    jobids = expand("{id:>04d}", id=list(range(n_macros)))
    return expand(pat, jobid=jobids, **kwargs, allow_missing=True)


def ver_filename_for_stp(config, simid):
    """Returns the vertices file needed for the 'stp' tier job, if needed. Used
    as lambda function in the `build_tier_stp` Snakemake rule."""
    sconfig = get_simconfig(config, "stp", simid)
    if "vertices" in sconfig:
        return output_simjob_filename(config, tier="ver", simid=sconfig.vertices)
    return []


# evt tier


def evtfile_rel_basename(**kwargs):
    return expand("{simid}/{simid}_{runid}-tier_evt", **kwargs, allow_missing=True)[0]


def output_evt_filename(config, **kwargs):
    expr = str(
        Path(config["paths"]["tier_evt"])
        / (evtfile_rel_basename() + FILETYPES["output"]["evt"])
    )
    return expand(expr, **kwargs, allow_missing=True)[0]


def log_evtfile_path(config, time, **kwargs):
    pat = str(
        Path(config["paths"]["log"]) / time, "evt" / (evtfile_rel_basename() + ".log")
    )
    return expand(pat, **kwargs, allow_missing=True)[0]


def benchmark_evtfile_path(config, **kwargs):
    pat = str(
        Path(config["paths"]["benchmarks"]) / "evt" / (evtfile_rel_basename() + ".tsv")
    )
    return expand(pat, **kwargs, allow_missing=True)[0]


# pdf tier


def pdffile_rel_basename(**kwargs):
    return expand("{simid}/{simid}-tier_pdf", **kwargs, allow_missing=True)[0]


def output_pdf_filename(config, **kwargs):
    expr = str(
        Path(config["paths"]["tier_pdf"])
        / (pdffile_rel_basename() + FILETYPES["output"]["pdf"])
    )
    return expand(expr, **kwargs, allow_missing=True)[0]


def log_pdffile_path(config, time, **kwargs):
    pat = str(
        Path(config["paths"]["log"]) / time / "pdf" / (pdffile_rel_basename() + ".log")
    )
    return expand(pat, **kwargs, allow_missing=True)[0]


def benchmark_pdffile_path(config, **kwargs):
    pat = str(
        Path(config["paths"]["benchmarks"]) / "pdf" / (pdffile_rel_basename() + ".tsv")
    )
    return expand(pat, **kwargs, allow_missing=True)[0]
