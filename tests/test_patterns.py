from __future__ import annotations

from pathlib import Path

from legendsimflow import patterns as p


def test_all(config):
    assert isinstance(p.simjob_rel_basename(config), str)
    assert isinstance(p.log_filename(config, "now"), Path)
    assert isinstance(p.plots_filepath(config), Path)
    assert isinstance(p.benchmark_filename(config), Path)
    assert isinstance(p.geom_gdml_filename(config), Path)
    assert isinstance(p.input_simjob_filename(config, tier="stp"), Path)
    assert isinstance(p.output_simjob_filename(config, tier="stp"), Path)
    assert isinstance(p.output_simjob_regex(config, tier="stp"), str)
    assert isinstance(p.input_simid_filenames(config, 2, tier="stp"), list)
    assert all(
        isinstance(p, Path) for p in p.input_simid_filenames(config, 2, tier="stp")
    )
    assert isinstance(p.output_simid_filenames(config, 2, tier="stp"), list)

    assert isinstance(p.output_dtmap_filename(config, hpge_detector="boh"), Path)
