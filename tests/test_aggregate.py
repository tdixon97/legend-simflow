from __future__ import annotations

from dbetto import AttrsDict

from legendsimflow import aggregate as agg


def test_simid_aggregates(config):
    assert agg.get_simid_njobs(config, "stp", "l200p03-birds-nest-K40") == 2
    assert isinstance(
        agg.gen_list_of_simid_inputs(config, "stp", "l200p03-birds-nest-K40"), list
    )
    assert isinstance(
        agg.gen_list_of_simid_outputs(config, "stp", "l200p03-birds-nest-K40"), list
    )


def test_simid_harvesting(config):
    simids = agg.gen_list_of_all_simids(config, "stp")
    assert isinstance(simids, type({}.keys()))
    assert all(isinstance(s, str) for s in simids)

    simcfgs = agg.collect_simconfigs(config, ["stp"])
    assert len(simcfgs) == 6


def test_simid_outputs(config):
    outputs = agg.gen_list_of_all_simid_outputs(config, "stp")
    assert isinstance(outputs, list)
    assert all(isinstance(s, str) for s in outputs)
    assert len(outputs) == sum(
        [
            agg.get_simid_njobs(config, "stp", s)
            for s in agg.gen_list_of_all_simids(config, "stp")
        ]
    )


def test_process_simlist(config):
    targets = agg.process_simlist(
        config,
        simlist=["stp.l200p03-birds-nest-K40", "stp.l200p03-pen-plates-Ra224-to-Pb208"],
    )
    assert targets == agg.gen_list_of_simid_outputs(
        config, "stp", "l200p03-birds-nest-K40"
    ) + agg.gen_list_of_simid_outputs(
        config, "stp", "l200p03-pen-plates-Ra224-to-Pb208"
    )


def test_dtmap_stuff(legend_metadata):
    cry = agg.crystal_meta(
        legend_metadata, legend_metadata.hardware.detectors.germanium.diodes.V99000A
    )
    assert isinstance(cry, AttrsDict)
    assert cry.name == "000"
    assert cry.order == "99"

    assert agg.start_key(legend_metadata, "l200-p02-r005-phy") == "20220602T000000Z"

    assert agg.gen_list_of_hpges_valid_for_dtmap(
        legend_metadata, "l200-p02-r005-phy"
    ) == ["V99000A"]
