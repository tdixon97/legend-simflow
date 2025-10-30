rule gen_all_tier_hit:
    """Aggregate and produce all the hit tier files."""
    input:
        aggregate.gen_list_of_all_simid_outputs(config, metadata, tier="hit"),


rule build_tier_hit:
    """Produces a hit tier file starting from a single `stp` tier file."""
    message:
        "Producing output file for job hit.{wildcards.simid}.{wildcards.jobid}"
    input:
        geom=patterns.geom_gdml_filename(config, tier="stp"),
        stp_file=patterns.output_simjob_filename(config, tier="stp"),
        optmap_lar=config.paths.optical_maps.lar,
    output:
        patterns.output_simjob_filename(config, tier="hit"),
    params:
        metadata=metadata,
    log:
        patterns.log_filename(config, proctime, tier="hit"),
    benchmark:
        patterns.benchmark_filename(config, tier="hit")
    script:
        "../src/legendsimflow/scripts/tier/hit.py"


def smk_hpge_drift_time_map_inputs(wildcards):
    meta = metadata.hardware.detectors.germanium.diodes[wildcards.hpge_detector]
    ids = {"bege": "B", "coax": "C", "ppc": "P", "icpc": "V"}
    crystal_name = (
        ids[meta.type] + format(meta.production.order, "02d") + meta.production.crystal
    )

    # remove the datatype at the end of the runid string, it's not needed to
    # locate the operational voltage file
    runid_no_dt = "-".join(wildcards.runid.split("-")[:-1])

    diode = (
        Path(config.paths.metadata)
        / f"hardware/detectors/germanium/diodes/{wildcards.hpge_detector}.yaml",
    )
    crystal = (
        Path(config.paths.metadata)
        / f"hardware/detectors/germanium/crystals/{crystal_name}.yaml"
    )
    opv = (
        Path(config.paths.metadata)
        / f"simprod/config/pars/opv/{runid_no_dt}-T%-all-opvs.yaml"
    )

    return {
        "detdb_file": diode,
        "crydb_file": crystal,
        "opv_file": opv,
    }


rule build_hpge_drift_time_map:
    """Produce an HPGe drift time map.

    Uses wildcards `hpge_detector` and `runid`.
    """
    message:
        "Generating drift time map for HPGe detector {wildcards.hpge_detector} in run {wildcards.runid}"
    input:
        unpack(smk_hpge_drift_time_map_inputs),
    output:
        temp(patterns.output_dtmap_filename(config)),
    log:
        patterns.log_dtmap_filename(config, proctime),
    threads: 4
    shell:
        "julia --project=. --threads {threads}"
        "  ../src/legendsimflow/scripts/make_hpge_drift_time_maps.jl"
        "    --detector {wildcards.hpge_detector}"
        f"   --metadata {config.paths.metadata}"
        "    --opv-file {input.opv_file}"
        "    --output-file {output} &> {log}"


rule merge_hpge_drift_time_maps:
    """Merge HPGe drift time maps in a single file.

    Uses wildcard `runid`.
    """
    message:
        "Merging HPGe drift time map files for {wildcards.runid}"
    input:
        lambda wc: aggregate.gen_list_of_dtmaps(config, metadata, wc.runid),
    output:
        patterns.output_dtmap_merged_filename(config),
    params:
        input_regex=patterns.output_dtmap_filename(config, hpge_detector="*"),
    shell:
        "cp $(ls {params.input_regex} | head -1) {output}; "
        "for f in $(ls {params.input_regex} | tail -n +2); do "
        "  for o in $(h5ls $f | awk '{{print $1}}'); do "
        "    h5copy -i $f -o {output} -s /$o -d /$o; "
        "  done"
        "done"
