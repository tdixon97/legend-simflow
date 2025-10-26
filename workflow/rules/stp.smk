from legendsimflow import aggregate, commands, utils
from dbetto import utils as dbetto_utils


rule gen_all_tier_stp:
    """Aggregate and produce all the stp tier files."""
    input:
        # aggregate.gen_list_of_all_plots_outputs(config, tier="stp"),
        aggregate.gen_list_of_all_simid_outputs(config, tier="stp"),


rule gen_geom_config:
    """Write a geometry configuration file for legend-pygeom-l200.

    Start from the template/default geometry configuration file and eventually
    add extra configuration options in case requested in simconfig.yaml.
    """
    message:
        "Generating geometry configuration for {tier}.{simid}"
    input:
        Path(config.paths.config) / "geom" / (config.experiment + "-geom-config.yaml"),
    output:
        patterns.geom_config(config),
    run:
        gconfig = dbetto_utils.load_dict(input[0])
        sconfig = utils.get_simconfig(
            config, tier=wildcards.tier, simid=wildcards.simid
        )

        if "geom_config_extra" in sconfig:
            gconfig |= sconfig.geom_config_extra.to_dict()

        dbetto_utils.write_dict(gconfig, output[0])


rule build_geom_gdml:
    message:
        "Building GDML geometry for {tier}.{simid}"
    input:
        patterns.geom_config(config),
    output:
        patterns.geom_gdml_filename(config),
    log:
        patterns.geom_log_filename(config, proctime),
    shell:
        "LEGEND_METADATA={config.paths.metadata} "
        "legend-pygeom-l200 --verbose --config {input} -- {output} &> {log}"


# since the number of jobs for the 'output' field must be deduced at runtime
# from the configuration, we need here to generate a separate rule for each
# 'simid'
simconfigs = aggregate.collect_simconfigs(config, ["stp"])
for _, simid in simconfigs:

    rule:
        """Run a single simulation job for the stp tier.
        Uses wildcards {simid} and `jobid`.
        """
        message:
            "Producing output file for job stp.{simid}.{wildcards.jobid}"
        input:
            verfile=patterns.ver_filename_for_stp(config, simid),
            geom=patterns.geom_gdml_filename(config, tier="stp", simid=simid),
        output:
            protected(patterns.output_simjob_filename(config, tier="stp", simid=simid)),
        log:
            patterns.log_filename(config, proctime, tier="stp", simid=simid),
        benchmark:
            patterns.benchmark_filename(config, tier="stp", simid=simid)
        threads: 1
        shell:
            commands.remage_run(config, simid, tier="stp", macro_free=True)

    ldfs.workflow.utils.set_last_rule_name(workflow, f"build_tier_stp_{simid}")
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
