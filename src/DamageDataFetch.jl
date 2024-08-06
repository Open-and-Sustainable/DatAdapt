module DamageDataFetch

using ExcelFiles
using DataFrames

export fetch_damage_data

function fetch_damage_data(start_year::Int, end_year::Int)
    filename = "data/raw/clima-hydro-meteo_$(start_year)-$(end_year)_EM-DAT.xlsx"
    return DataFrame(load(filename, "EM-DAT Data"))
end

end
