# Copyright (C) 2025 Luigi Pertoldi <gipert@pm.me>
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
"""Spectra on a grid of parameters, in the layout Refit.jl reads as a generalised histogram."""

from __future__ import annotations

import itertools
from collections.abc import Callable, Mapping

import hist
import numpy as np
from lgdo import Histogram, Scalar, Struct


def build_pdf_grid(
    pdf: Callable[
        [dict[str, float]], hist.Hist | Histogram | Mapping[str, hist.Hist | Histogram]
    ],
    axes: Mapping[str, tuple[float, float, float]],
) -> Struct | dict[str, Struct]:
    """Evaluate a PDF at every point of an evenly spaced grid of parameters.

    The result is an LGDO struct that Refit.jl reads as a generalised histogram:
    a ``grid_info`` struct with the name and the ``first``, ``last`` and ``step`` values of each
    parameter (``axis_0``, ``axis_1``, ...), and a ``grid`` struct with the
    histogram of each grid point at ``grid/<name0>_<i>/<name1>_<j>/...``,
    nested in the order of `axes`, with indices counted from 0. Write it with
    ``lh5.write``.

    Parameters
    ----------
    pdf
        function of the parameter values, given as a dictionary ``{name:
        value}``, that returns the histogram at this grid point, or a dictionary
        of histograms ``{key: histogram}`` to build several PDFs in one scan.
        The histograms of a PDF must have the same binning at all grid points
        and hold raw counts, from the same number of simulated primaries.
    axes
        for each parameter, in the order of the grid axes, its ``(first, last,
        step)`` values.

    Returns
    -------
    the grid, or a dictionary ``{key: grid}`` if `pdf` returns a dictionary.

    Examples
    --------
    >>> grid = build_pdf_grid(pdf, {"fccd_sigma": (-5, 5, 1), "dlf": (0, 1, 0.25)})
    >>> lh5.write(grid, "pdf_grid/mul1/all", "grid.lh5")
    """
    grid_info = Struct()
    values = {}
    for d, (name, (first, last, step)) in enumerate(axes.items()):
        values[name] = first + step * np.arange(round((last - first) / step) + 1)
        grid_info.add_field(f"axis_{d}/name", Scalar(name))
        for key, value in (("first", first), ("last", last), ("step", step)):
            grid_info.add_field(f"axis_{d}/values/{key}", Scalar(float(value)))

    grids = {}
    for idx in itertools.product(*(range(len(v)) for v in values.values())):
        hists = pdf(
            {name: float(values[name][i]) for name, i in zip(values, idx, strict=True)}
        )
        if not isinstance(hists, Mapping):
            hists = {None: hists}

        point = "/".join(f"{name}_{i}" for name, i in zip(values, idx, strict=True))
        for key, h in hists.items():
            if key not in grids:
                grids[key] = Struct({"grid_info": grid_info, "grid": Struct()})
            grids[key]["grid"].add_field(
                point, h if isinstance(h, Histogram) else Histogram(h)
            )

    for grid in grids.values():
        validate_grid(grid)

    # one grid if pdf returns one histogram
    return grids.get(None, grids)


def validate_grid(grid: Struct) -> None:
    """Check a grid as returned by :func:`build_pdf_grid`.

    Raises a :class:`ValueError` if an axis does not go from ``first`` to
    ``last`` in whole steps, if a grid point has no histogram, or if the
    histograms have different binnings.

    Parameters
    ----------
    grid
        the grid, built or read from a file with ``lh5.read``.
    """
    n_values = {}
    for axis in grid["grid_info"].values():
        name = axis["name"].value
        name = name.decode() if isinstance(name, bytes) else name  # bytes when read
        first, last, step = (axis["values"][k].value for k in ("first", "last", "step"))
        n_values[name] = round((last - first) / step) + 1
        if n_values[name] < 2 or not np.isclose(
            first + (n_values[name] - 1) * step, last
        ):
            msg = f"axis {name} does not go from {first} to {last} in steps of {step}"
            raise ValueError(msg)

    edges = None
    for idx in itertools.product(*(range(n) for n in n_values.values())):
        point = "/".join(f"{name}_{i}" for name, i in zip(n_values, idx, strict=True))
        if point not in grid["grid"]:
            msg = f"grid point {point} has no histogram"
            raise ValueError(msg)

        point_edges = [ax.edges for ax in grid["grid"][point].binning]
        if edges is None:
            edges = point_edges
        elif len(point_edges) != len(edges) or not all(
            np.array_equal(a, b) for a, b in zip(point_edges, edges, strict=False)
        ):
            msg = f"the histogram at grid point {point} has a different binning"
            raise ValueError(msg)
