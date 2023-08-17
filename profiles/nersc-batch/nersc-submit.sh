#!/bin/bash

function make_version() {
    version="$(basename "$PWD")"
    echo "$(basename "$(dirname "$PWD")")-$version"
}

function sbatch_submit() {
    local job="$1"
    local logdir="$2"
    shift 2

    echo "INFO: submitting $job..."
    sbatch \
        --nodes 1 \
        --ntasks-per-node=1 \
        --account m2676 \
        --constraint cpu \
        --time 12:00:00 \
        --qos regular \
        --licenses scratch,cfs \
        --job-name "$job" \
        --output "$logdir/$job.log" \
        --error "$logdir/$job.log" \
        --wrap "
            srun snakemake \
                --shadow-prefix $PSCRATCH \
                $*
        "
}

function sbatch_submit_simid() {
    local simid="$1"
    local job
    job="$(make_version)::$simid"

    echo "INFO: inspecting $job"
    is_job_in_queue "$job" && return

    snakemake --config simlist="$simid" --dry-run 2> /dev/null | grep 'Nothing to be done' && return

    sbatch_submit "$job" "$logdir" --config simlist="$simid"
}

function is_job_in_queue() {
    if squeue --me --format '%200j' | grep "$1"; then
        echo "INFO: job already queued"
        return 0
    else return 1
    fi
}

export -f make_version
export -f sbatch_submit
export -f sbatch_submit_simid
export -f is_job_in_queue

logdir=".slurm/$(date +'%Y%m%dT%H%M%SZ')"
mkdir -p "$logdir"

if [[ "$1" == "parallel" ]]; then
    simids=$(python -c '
import json

with open("inputs/simprod/config/tier/raw/l200a/simconfig.json") as f:
    simids = json.load(f).keys()

for s in simids:
    print(f"pdf.{s}", end=" ")
    ')

    # shellcheck disable=SC2086
    parallel --citation --jobs 10 sbatch_submit_simid ::: $simids

else
    job="$(make_version)::all"

    echo "INFO: inspecting $job"
    is_job_in_queue "$job" && exit 1

    snakemake --dry-run "$@" 2> /dev/null | grep 'Nothing to be done' && exit 1

    sbatch_submit "$job" "$logdir" "$*"
fi

echo "INFO: logs in $(realpath "$logdir")"
