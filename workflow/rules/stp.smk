"""Rules to build the `stp` tier.
"""

from legendsimflow import aggregate, commands


rule gen_all_tier_stp:
    """Build the entire `stp` tier."""
    input:
        # aggregate.gen_list_of_all_plots_outputs(config, tier="stp"),
        aggregate.gen_list_of_all_simid_outputs(config, tier="stp"),


rule gen_geom_config:
    """Write a geometry configuration file for legend-pygeom-l200.

    Start from the template/default geometry configuration file and eventually
    add extra configuration options in case requested in `simconfig.yaml`
    through the `geom_config_extra` field.
    """
    message:
        "Generating geometry configuration for {wildcards.tier}.{wildcards.simid}"
    input:
        Path(config.paths.config) / "geom" / (config.experiment + "-geom-config.yaml"),
    output:
        patterns.geom_config(config),
    run:
        from dbetto import utils as dbetto_utils

        gconfig = dbetto_utils.load_dict(input[0])
        sconfig = utils.get_simconfig(
            config, tier=wildcards.tier, simid=wildcards.simid
        )

        if "geom_config_extra" in sconfig:
            gconfig |= sconfig.geom_config_extra.to_dict()

        dbetto_utils.write_dict(gconfig, output[0])


rule build_geom_gdml:
    """Build a concrete geometry GDML file with {mod}`legend-pygeom-l200`."""
    message:
        "Building GDML geometry for {wildcards.tier}.{wildcards.simid}"
    input:
        patterns.geom_config(config),
    output:
        patterns.geom_gdml_filename(config),
    log:
        patterns.geom_log_filename(config, proctime),
    shell:
        "LEGEND_METADATA={config.paths.metadata} "
        "legend-pygeom-l200 --verbose --config {input} -- {output} &> {log}"


def smk_remage_run(wildcards, input, output, threads):
    """Generate the remage command line for use in Snakemake rules."""
    return commands.remage_run(
        config,
        wildcards.simid,
        tier="stp",
        geom=input.geom,
        output=output,
        threads=threads,
        macro_free=True,
    )


rule build_tier_stp:
    """Run a single simulation job for the `stp` tier.

    Uses wildcards `simid` and `jobid`.

    :::{note}
    The output remage file is declared as `protected` to avoid accidental
    deletions, since it typically takes a lot of resources to produce it.
    :::
    """
    message:
        "Producing output file for job stp.{wildcards.simid}.{wildcards.jobid}"
    input:
        # verfile=patterns.ver_filename_for_stp(config, sid),
        geom=patterns.geom_gdml_filename(config, tier="stp"),
    output:
        protected(patterns.output_simjob_filename(config, tier="stp")),
    log:
        patterns.log_filename(config, proctime, tier="stp"),
    benchmark:
        patterns.benchmark_filename(config, tier="stp")
    threads: 1
    params:
        cmd=smk_remage_run,
    shell:
        "{params.cmd} &> {log}"


# rule:
#     """Produces plots for the primary event vertices of simid {simid} in tier {tier}"""
#     input:
#         aggregate.gen_list_of_simid_outputs(config, tier, simid, max_files=5),
#     output:
#         Path(patterns.plots_filepath(config, tier=tier, simid=simid))
#         / f"mage-event-vertices-tier_{tier}.png",
#     priority: 100  # prioritize producing the needed input files over the others
#     shell:
#         (
#             " ".join(config["execenv"])
#             + " python "
#             + workflow.source_path("../scripts/plot_mage_vertices.py")
#             + " -b -o {output} {input}"
#         )
# ldfs.workflow.utils.set_last_rule_name(workflow, f"plot_prim_vert_{simid}-tier_{tier}")
