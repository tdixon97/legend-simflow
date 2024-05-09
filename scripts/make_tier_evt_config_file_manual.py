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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# NOTE: this script is not ready to be integrated in Snakemake, since it only
# works on the LNGS cluster! At the end of this file there's an example of how
# it should look like

# ruff: noqa: F821, T201


import json
import math
import argparse

from pathlib import Path

from legendmeta import JsonDB, LegendMetadata

### add an args parsers
parser = argparse.ArgumentParser(description="Create evt tier config files for LEGEND simulation production"
)

parser.add_argument("--metadata", "-m", type=str, help="Path to proudction cycle to take legend-metadata",default="/data2/public/prodenv/prod-blind/ref-v1.1.0/")
parser.add_argument("--prod_dir", "-p", type=str, help="Path to proudction cycle to take legend-metadata",default="/data2/public/prodenv/prod-blind/ref-v1.0.1/")
parser.add_argument("--hit_name", "-H", type=str, help="Name for the hit tier - either hit or pht",default="pht")
parser.add_argument("--dataset", "-d", type=str, help="Dataset to produce evt tier cfg for 'vancouver', 'p10', or 'p02'",default="vancouver")

args= parser.parse_args()

prod_dir =args.prod_dir
metadata = args.metadata
hit_name =args.hit_name
dataset=args.dataset

lmeta = LegendMetadata(f"{metadata}/inputs")
runs=lmeta.dataprod.config.analysis_runs
print(runs)
if (dataset=="vancouver"):
    runlist=[]
    for per in runs.keys():
        for run in runs[per]:
            runlist.append(f"l200-{per}-{run}-phy")
    runlist=(runlist)
elif (dataset=="p10"):
    runlist = []
    runs["p10"]=["r000","r001","r003","r004","r005","r006"]
    for run in runs["p10"]:
        runlist.append(f"l200-p10-{run}-phy")
elif (dataset=="p02"):
    runlist = (
        [f"l200-p02-r{r:03d}-phy" for r in range(13,14)]
      )

# use FCCD values reviewed by Elisabetta
with Path("fccd-reviewed.json").open() as f:
    fccd = json.load(f)["fccd-mm"]

# get generated parameters (for energy resolution) from the data production
par_pht_meta = JsonDB(f"{prod_dir}/generated/par/{hit_name}", lazy=True)

for run in runlist:
    print(">>>", run)

    # get parameters and hardware configuration for run
    p = run.split("-")
    if (p[3] not in lmeta.dataprod.runinfo[p[1]][p[2]]):
        continue
    tstamp = lmeta.dataprod.runinfo[p[1]][p[2]][p[3]].start_key
    chmap = lmeta.channelmap(tstamp).map("system", unique=False).geds
    hit_pars = par_pht_meta.on(tstamp)

    # now loop over all HPGe channels
    evt_cfg = {}
    for _, data in chmap.items():
        # compute the MaGe sensitive volume identifier
        mage_id = int(1010000 + 100 * data.location.string + data.location.position)

        # get energy resolution curves
        eres_pars = [None, None, None]
        channel = f"ch{data.daq.rawid}"
        if channel in hit_pars:
            pars = None
            # first try getting curves specific to the run
            # i.e. parameters of sqrt(a + b*E)
            try:
                ene_cfg = hit_pars[channel].results.ecal.cuspEmax_ctc_cal
                pars = [*list(ene_cfg.eres_linear.parameters.values()), 0]
            except AttributeError:
                # if not found, use curves specific to partition
                try:
                    pars = hit_pars[
                        channel
                    ].results.partition_ecal.cuspEmax_ctc_cal.eres_linear.parameters.values()
                except AttributeError:
                    if data.analysis.usability not in ("off", "ac"):
                        print(
                            f"WARNING: no eres params found for {data.analysis.usability} {channel}/{data.name} in {run}"
                        )
            
            # convert to format expected by mage-post-proc
            if pars is not None and all(p >= 0 for p in pars):
                eres_pars = [round(math.sqrt(x) / 2.355, 6) for x in pars]

                if (len(eres_pars)==2):
                    eres_pars.append(0)
            else:
                eres_pars=[2,0,0]

        elif data.analysis.usability not in ("off", "ac"):
            print(
                f"ERROR: {data.analysis.usability} {channel}/{data.name} not in JSON file"
            )

        # finally add JSON block about channel
        evt_cfg[mage_id] = {
            "name": data.name,
            "nplus-fccd-mm": fccd[data.name],
            "energy": dict(zip(["sig0", "sig1", "sig2"], eres_pars)),
            "usability": data.analysis.usability,
        }

    with Path(f"{run}-build_evt.json").open("w") as f:
        json.dump(evt_cfg, f, indent=2)


# import os
# from pathlib import Path

# # TODO: generate the files from metadata inplace here
# # this script is run for each data taking run
# # snakemake.input[0] points to the legend-metadata clone in legend-simflow
# # snakemake.output[0] is the output filename

# src = (
#     Path(snakemake.input[0])
#     / "simprod"
#     / "config"
#     / "tier"
#     / "evt"
#     / snakemake.config["experiment"]
#     / f"{snakemake.wildcards.runid}-build_evt.json"
# )
# dest = snakemake.output[0]

# os.system(f"cp {src} {dest}")
