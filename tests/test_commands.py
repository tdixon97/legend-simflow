from __future__ import annotations

import shlex
from pathlib import Path

import conftest
import pytest

from legendsimflow import SimflowConfigError, commands, patterns


def test_make_macro(config):
    text, fmac = commands.make_remage_macro(config, "l200p03-birds-nest-K40", "stp")

    assert fmac == Path(config.paths.macros) / "stp/l200p03-birds-nest-K40-tier_stp.mac"
    assert fmac.is_file()

    assert set(config.metadata.tier.stp.l200p03.confinement["birds-nest"]).issubset(
        text.split("\n")
    )
    assert set(config.metadata.tier.stp.l200p03.generators["K40"]).issubset(
        text.split("\n")
    )

    text, fmac = commands.make_remage_macro(
        config, "l200p03-pen-plates-Ra224-to-Pb208", "stp"
    )
    assert set(config.metadata.tier.stp.l200p03.generators["Ra224-to-Pb208"]).issubset(
        text.split("\n")
    )

    confine = [
        "/RMG/Generator/Confine Volume",
        "/RMG/Generator/Confinement/Physical/AddVolume pen.*",
    ]
    assert set(confine).issubset(text.split("\n"))
    assert "/RMG/Generator/Confine/SampleOnSurface" not in text

    text, fmac = commands.make_remage_macro(config, "l200p03-hpge-surface-K42", "stp")
    confine = [
        "/RMG/Generator/Confine Volume",
        "/RMG/Generator/Confinement/Physical/AddVolume V.*",
        "/RMG/Generator/Confinement/Physical/AddVolume B.*",
        "/RMG/Generator/Confine/SampleOnSurface true",
    ]
    assert set(confine).issubset(text.split("\n"))


def test_make_macro_errors():
    config = conftest.make_config()

    config.metadata.tier.stp.l200p03.simconfig["l200p03-birds-nest-K40"][
        "generator"
    ] = "coddue"
    with pytest.raises(SimflowConfigError):
        commands.make_remage_macro(config, "l200p03-birds-nest-K40", "stp")

    config.metadata.tier.stp.l200p03.simconfig["l200p03-birds-nest-K40"][
        "generator"
    ] = "~coddue:boh"
    with pytest.raises(SimflowConfigError):
        commands.make_remage_macro(config, "l200p03-birds-nest-K40", "stp")

    config.metadata.tier.stp.l200p03.simconfig["l200p03-birds-nest-K40"][
        "generator"
    ] = "~defines:boh"
    with pytest.raises(SimflowConfigError):
        commands.make_remage_macro(config, "l200p03-birds-nest-K40", "stp")

    config = conftest.make_config()
    config.metadata.tier.stp.l200p03.simconfig["l200p03-birds-nest-K40"][
        "confinement"
    ] = "~baaaaaa:beh"

    with pytest.raises(SimflowConfigError):
        commands.make_remage_macro(config, "l200p03-birds-nest-K40", "stp")

    config.metadata.tier.stp.l200p03.simconfig["l200p03-birds-nest-K40"][
        "confinement"
    ] = "~defines:beh"
    with pytest.raises(SimflowConfigError):
        commands.make_remage_macro(config, "l200p03-birds-nest-K40", "stp")

    config.metadata.tier.stp.l200p03.simconfig["l200p03-birds-nest-K40"][
        "confinement"
    ] = {}
    with pytest.raises(SimflowConfigError):
        commands.make_remage_macro(config, "l200p03-birds-nest-K40", "stp")


def test_remage_cli(config):
    cmd = commands.remage_run(config, "l200p03-birds-nest-K40", "stp")
    assert isinstance(cmd, str)
    assert len(cmd) > 0
    assert shlex.split(cmd)[-1] == patterns.input_simjob_filename(config, tier="stp")

    cmd = commands.remage_run(config, "l200p03-birds-nest-K40", "stp", macro_free=True)
    mac_cmds = shlex.split(cmd.partition(" -- ")[2])
    assert all(cmd[0] == "/" for cmd in mac_cmds)
