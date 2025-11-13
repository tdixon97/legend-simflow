import Pkg

if !ispath(joinpath(first(DEPOT_PATH), "registries", "LegendJuliaRegistry"))
    @info("Installing Legend Julia package registry")
    Pkg.Registry.add("General")
    Pkg.Registry.add(url = "https://github.com/legend-exp/LegendJuliaRegistry.git")
end

Pkg.instantiate()
Pkg.precompile()
