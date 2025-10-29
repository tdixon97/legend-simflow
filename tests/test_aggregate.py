from __future__ import annotations

import conftest
from dbetto import AttrsDict

from legendsimflow import aggregate as agg


def test_simid_aggregates(config, metadata):
    assert agg.get_simid_njobs(config, metadata, "stp", "l200p03-birds-nest-K40") == 2
    assert isinstance(
        agg.gen_list_of_simid_inputs(config, metadata, "stp", "l200p03-birds-nest-K40"),
        list,
    )
    assert isinstance(
        agg.gen_list_of_simid_outputs(
            config, metadata, "stp", "l200p03-birds-nest-K40"
        ),
        list,
    )

    config_bench = conftest.make_config()
    config_bench.benchmark.enabled = True
    config_bench.benchmark.n_primaries.stp = 999

    assert (
        agg.get_simid_njobs(config_bench, metadata, "stp", "l200p03-birds-nest-K40")
        == 1
    )


def test_simid_harvesting(config, metadata):
    simids = agg.gen_list_of_all_simids(config, metadata, "stp")
    assert isinstance(simids, type({}.keys()))
    assert all(isinstance(s, str) for s in simids)

    simcfgs = agg.collect_simconfigs(config, metadata, ["stp"])
    assert len(simcfgs) == 6


def test_simid_outputs(config, metadata):
    outputs = agg.gen_list_of_all_simid_outputs(config, metadata, "stp")
    assert isinstance(outputs, list)
    assert all(isinstance(s, str) for s in outputs)
    assert len(outputs) == sum(
        [
            agg.get_simid_njobs(config, metadata, "stp", s)
            for s in agg.gen_list_of_all_simids(config, metadata, "stp")
        ]
    )


def test_process_simlist(config, metadata):
    targets = agg.process_simlist(
        config,
        metadata,
        simlist=["stp.l200p03-birds-nest-K40", "stp.l200p03-pen-plates-Ra224-to-Pb208"],
    )
    assert targets == agg.gen_list_of_simid_outputs(
        config, metadata, "stp", "l200p03-birds-nest-K40"
    ) + agg.gen_list_of_simid_outputs(
        config, metadata, "stp", "l200p03-pen-plates-Ra224-to-Pb208"
    )


def test_dtmap_stuff(legend_test_metadata):
    m = legend_test_metadata
    cry = agg.crystal_meta(m, m.hardware.detectors.germanium.diodes.V99000A)
    assert isinstance(cry, AttrsDict)
    assert cry.name == "000"
    assert cry.order == "99"

    assert agg.start_key(m, "l200-p02-r005-phy") == "20220602T000000Z"

    assert agg.gen_list_of_hpges_valid_for_dtmap(m, "l200-p02-r005-phy") == ["V99000A"]
