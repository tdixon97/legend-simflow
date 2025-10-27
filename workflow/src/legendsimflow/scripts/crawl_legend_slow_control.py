# Copyright (C) 2025 Luigi Pertoldi <gipert@pm.me>,
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
from __future__ import annotations

import argparse
import logging

from dbetto import utils
from legendmeta import LegendMetadata, LegendSlowControlDB

parser = argparse.ArgumentParser()

parser.add_argument("runsel", help="run selection string, i.e. l200-p14-r001-phy")
parser.add_argument("-o", "--output", help="output file path", required=True)

args = parser.parse_args()


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

log.info("initializing...")

lmeta = LegendMetadata()
scdb = LegendSlowControlDB()
scdb.connect()

experiment, period, run, datatype = args.runsel.split("-")

timestamp = lmeta.datasets.runinfo[period][run][datatype].start_key
msg = f"start timestamp of {args.runsel} is {timestamp}"
log.info(msg)

chmap = lmeta.channelmap(timestamp).group("system").geds

log.info("querying the LEGEND Slow Control...")

voltages = {}
for _, meta in chmap.items():
    vset = scdb.status(meta, on=timestamp).vset

    msg = f"voltage set for channel {meta.name} is {vset} V"
    logging.info(msg)

    voltages[meta.name] = {"operational_voltage_in_V": vset}

utils.write_dict(voltages, args.output)
