module DatAdapt

using DataFrames
#using DuckDB

# Include and use the renamed modules
include("DatabaseAccess.jl")
include("ExposureDataFetch.jl")
include("DamageDataFetch.jl")
#include("DataCleaning.jl")

using .DatabaseAccess
using .ExposureDataFetch
using .DamageDataFetch

#using .DataCleaning


export fetch_exposure_data

function fetch_exposure_data()
    # wb_test_data = DataFetch.fetch_WB_test_data()
    # write_duckdb_table(wb_test_data, db_path, "wb_test_data")
    db_path = "data/raw/DatAdapt_1980-2021.duckdb"
    E_data = ExposureDataFetch.fetch_exposure_data(1980, 2021)
    DatabaseAccess.write_duckdb_table!(E_data, db_path, "exposure")
end

function fetch_damage_data()
    db_path = "data/raw/DatAdapt_1980-2021.duckdb"
    D_data = ExposureDataFetch.fetch_damage_data(1980, 2021)
    DatabaseAccess.write_duckdb_table!(D_data, db_path, "damage")
end

function clean_data(df::DataFrame)
    return DataCleaning.clean_data(df)
end

function transform_data(df::DataFrame)
    return DataCleaning.transform_data(df)
end

function setup_database()
    DatabaseSetup.setup()
end

end # module DatAdapt
