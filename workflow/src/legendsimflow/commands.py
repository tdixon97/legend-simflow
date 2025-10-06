from __future__ import annotations

import shlex
from pathlib import Path

import legenddataflowscripts as lds
import numpy as np

from . import patterns
from .exceptions import SimflowConfigError
from .utils import get_simconfig


def remage_run(config, simid, tier="stp", macro_free=False):
    """remage command line for Snakemake rules."""
    # get the config block for this tier/simid
    block = f"metadata.tier.{tier}.{config.experiment}.simconfig.{simid}"
    sim_cfg = get_simconfig(config, tier, simid=simid)

    # get macro
    macro_text, _ = make_remage_macro(config, simid, tier=tier)

    # need some modifications if this is a benchmark run
    try:
        n_prim_pj = sim_cfg.primaries_per_job
        is_benchmark = False

        if "benchmark" in config:
            is_benchmark = config.benchmark.get("enabled", False)
            if is_benchmark:
                n_prim_pj = config.benchmark.n_primaries[tier]
    except KeyError as e:
        raise SimflowConfigError(block, e) from e

    # substitution rules
    cli_subs = {
        "N_EVENTS": int(n_prim_pj),
        # TODO: check correct range
        "SEED": np.random.default_rng().integers(
            0, np.iinfo(np.int32).max + 1, dtype=np.uint32
        ),
    }

    # prepare command line
    if "runcmd" in config and "remage" in config.runcmd:
        remage_exe = shlex.split(config.runcmd.get("remage", "remage"))
    else:
        remage_exe = ["remage"]

    cmd = [
        *remage_exe,
        "--merge-output-files",
        "--log-level=detail",
        "--threads",
        "1",
        "--gdml-files",
        str(patterns.pygeom_filename(config)),
        "--output-file",
        str(patterns.output_simjob_filename(config, tier=tier, simid=simid)),
    ]

    if macro_free:
        # actually substitute values in the command line
        for k, v in cli_subs.items():
            macro_text = macro_text.replace(f"{{{k}}}", str(v))

        cmd += ["--"]

        # remove empty lines and comments if passing macro commands to the cli
        cmd += [
            line for line in macro_text.splitlines() if line.strip() and line[0] != "#"
        ]
    else:
        # otherwise just pass the macro file
        cmd += ["--macro-substitutions"]
        cmd += [shlex.quote(f"{k}={v}") for k, v in cli_subs.items()]

        cmd += ["--"]

        # NOTE: macro file path contains unexpanded wildcards!
        cmd += [patterns.input_simjob_filename(config, tier=tier)]

    return shlex.join(cmd)


def make_remage_macro(config, simid, tier="stp"):
    # get the config block for this tier/simid
    block = f"metadata.tier.{tier}.{config.experiment}.simconfig.{simid}"
    sim_cfg = get_simconfig(config, tier, simid=simid)
    mac_subs = {}

    # determine whether external vertices are required
    if "vertices" in sim_cfg:
        raise NotImplementedError()
        mac_subs.update({"VERTICES_FILE": None})

    # configure generator
    if "generator" in sim_cfg:
        if not isinstance(sim_cfg.generator, str) or not sim_cfg.generator.startswith(
            "~defines:"
        ):
            msg = (
                f"{block}.generator",
                "the field must be a string prefixed by ~define:",
            )
            raise SimflowConfigError(*msg)

        key = sim_cfg.generator.removeprefix("~defines:")
        try:
            generator = config.metadata.tier[tier][config.experiment].generators[key]
        except KeyError as e:
            raise SimflowConfigError(block, e) from e

        if not isinstance(generator, str):
            generator = "\n".join(generator)

        mac_subs["GENERATOR"] = generator

    # configure generator
    if "confinement" in sim_cfg:
        confinement = None
        if isinstance(sim_cfg.confinement, str):
            if sim_cfg.confinement.startswith("~defines:"):
                key = sim_cfg.confinement.removeprefix("~defines:")
                try:
                    confinement = config.metadata.tier[tier][
                        config.experiment
                    ].confinement[key]
                except KeyError as e:
                    raise SimflowConfigError(block, e) from e

            elif sim_cfg.confinement.startswith(
                ("~volumes.surface:", "~volumes.bulk:")
            ):
                confinement = [
                    "/RMG/Generator/Confine Volume",
                    "/RMG/Generator/Confinement/Physical/AddVolume "
                    + sim_cfg.confinement.partition(":")[2],
                ]
                if sim_cfg.confinement.startswith("~volumes.surface:"):
                    confinement += ["/RMG/Generator/Confine/SampleOnSurface true"]
            else:
                confinement = None

        elif isinstance(sim_cfg.confinement, list | tuple):
            confinement = ["/RMG/Generator/Confine Volume"]
            for val in sim_cfg.confinement:
                if val.startswith(("~volumes.surface:", "~volumes.bulk:")):
                    confinement += [
                        "/RMG/Generator/Confinement/Physical/AddVolume "
                        + val.partition(":")[2]
                    ]
                    if val.startswith("~volumes.surface:"):
                        confinement += ["/RMG/Generator/Confine/SampleOnSurface true"]
                else:
                    confinement = None

        if confinement is None:
            msg = (
                f"{block}.confinement",
                (
                    "the field must be a str or list[str] prefixed by "
                    "~define: / ~volumes.surface: / ~volumes.bulk:"
                ),
            )
            raise SimflowConfigError(*msg)

        if not isinstance(confinement, str):
            confinement = "\n".join(confinement)

        mac_subs["CONFINEMENT"] = confinement

    # read in template and substitute
    template_path = get_simconfig(config, tier, simid=simid, field="template")
    with Path(template_path).open() as f:
        text = lds.subst_vars(f.read().strip(), mac_subs, ignore_missing=False)

    # now write the macro to disk
    ofile = Path(patterns.input_simjob_filename(config, tier=tier, simid=simid))
    ofile.parent.mkdir(parents=True, exist_ok=True)
    with ofile.open("w") as f:
        f.write(text)

    return text, ofile
