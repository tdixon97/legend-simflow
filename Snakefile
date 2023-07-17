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
# along with this program.  If not, see <https:#www.gnu.org/licenses/>.

from pathlib import Path

from scripts.utils import utils, patterns, aggregate

if not config:
    raise RuntimeError("you must set a config file with --configfile")

utils.subst_vars_in_snakemake_config(workflow, config)

swenv = " ".join(config["execenv"])
config.setdefault("benchmark", {"enabled": False})


wildcard_constraints:
    tier="\w+",
    simid="[-\w]+",
    jobid="\w+",


def gen_target_all():
    if config.get("simlist", "all") == "all":
        return (
            aggregate.gen_list_of_all_simid_outputs(config, tier="hit"),
            aggregate.gen_list_of_all_plots_outputs(config, tier="raw"),
        )
    else:
        return aggregate.process_simlist_or_all(config)


rule all:
    input:
        gen_target_all(),


rule gen_all_macros:
    """Aggregate and produce all the macro files."""
    input:
        aggregate.gen_list_of_all_macros(config, tier="ver"),
        aggregate.gen_list_of_all_macros(config, tier="raw"),


rule gen_all_tier_raw:
    """Aggregate and produce all the 'raw' tier files."""
    input:
        aggregate.gen_list_of_all_plots_outputs(config, tier="raw"),
        aggregate.gen_list_of_all_simid_outputs(config, tier="raw"),


rule gen_all_tier_hit:
    """Aggregate and produce all the 'hit' tier files."""
    input:
        aggregate.gen_list_of_all_simid_outputs(config, tier="hit"),


# since the number of generated macros for the 'output' field
# must be deduced at runtime from the JSON configuration, we need here to
# generate a separate rule for each 'simid'
simconfigs = aggregate.collect_simconfigs(config, ["ver", "raw"])

for tier, simid, n_macros in simconfigs:

    rule:
        f"""Generates all needed simulation macros ({n_macros})
        for {simid} in tier '{tier}'. No wildcards are used.
        """
        localrule: True
        input:
            **patterns.macro_gen_inputs(config, tier, simid),
        output:
            patterns.input_simid_filenames(config, n_macros, tier=tier, simid=simid),
        params:
            tier=tier,
            simid=simid,
        threads: 1
        message:
            f"Generating macros for '{tier}.{simid}'"
        script:
            "scripts/generate_macros.py"


rule build_tier_ver:
    """Run a single simulation job for the 'ver' tier.
    Uses wildcards `simid` and `jobid`.

    Warning
    -------
    The macro file is marked as "ancient" as a workaround to the fact that
    it might have been re-generated (i.e. it effectively has a more recent
    creation time) but with the same content as before (i.e. there is no need
    to re-run the simulation). If the macro content is updated, users will need
    to manually remove the output simulation files or force execution.
    """
    message:
        "Producing output file for job 'ver.{simid}.{jobid}'"
    input:
        macro=ancient(patterns.input_simjob_filename(config, tier="ver")),
    output:
        protected(patterns.output_simjob_filename(config, tier="ver")),
    log:
        patterns.log_file_path(config, tier="ver"),
    benchmark:
        patterns.benchmark_file_path(config, tier="ver")
    shadow:
        "minimal"
    shell:
        patterns.run_command(config, "ver")


rule build_tier_raw:
    """Run a single simulation job for the 'raw' tier.
    Uses wildcards `simid` and `jobid`.

    Warning
    -------
    The macro file is marked as "ancient" as a workaround to the fact that
    it might have been re-generated (i.e. it effectively has a more recent
    creation time) but with the same content as before (i.e. there is no need
    to re-run the simulation). If the macro content is updated, users will need
    to manually remove the output simulation files or force execution.
    """
    message:
        "Producing output file for job 'raw.{simid}.{jobid}'"
    input:
        macro=ancient(patterns.input_simjob_filename(config, tier="raw")),
        verfile=lambda wildcards: patterns.smk_ver_filename_for_raw(config, wildcards),
    output:
        protected(patterns.output_simjob_filename(config, tier="raw")),
    log:
        patterns.log_file_path(config, tier="raw"),
    benchmark:
        patterns.benchmark_file_path(config, tier="raw")
    shadow:
        "minimal"
    shell:
        patterns.run_command(config, "raw")


for tier, simid, _ in simconfigs:

    rule:
        """Produces plots for the primary event vertices."""
        input:
            aggregate.gen_list_of_simid_outputs(config, tier, simid, max_files=5),
        output:
            patterns.plots_file_path(config, tier=tier, simid=simid)
            + "/mage-event-vertices.png",
        params:
            prog=workflow.source_path("scripts/plot_mage_vertices.py"),
        priority: 100
        shell:
            "{swenv} python {params.prog} -b -o {output} {input}"


rule build_tier_hit:
    """Produces a 'hit' tier file starting from a single 'raw' tier file."""
    message:
        "Producing output file for job 'hit.{simid}.{jobid}'"
    input:
        rules.build_tier_raw.output,
    output:
        patterns.output_simjob_filename(config, tier="hit"),
    log:
        patterns.log_file_path(config, tier="hit"),
    benchmark:
        patterns.benchmark_file_path(config, tier="hit")
    shadow:
        "minimal"
    shell:
        patterns.run_command(config, "hit")


rule print_stats:
    """Prints a table with summary runtime information for each `simid`.
    No wildcards are used.
    """
    script:
        "scripts/print_simprod_stats.py"


rule print_benchmark_stats:
    """Prints a table with summary runtime information of a benchmarking run.
    No wildcards are used.
    """
    script:
        "scripts/print_benchmark_stats.py"
