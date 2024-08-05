module DatAdapt

using DataFrames
using DuckDB

# Include and use the renamed modules
include("DataFetch.jl")
include("DataCleaning.jl")
include("DatabaseSetup.jl")
using .DataFetch
using .DataCleaning
using .DatabaseSetup

# Core functions and types
export fetch_all_data, clean_data, transform_data, setup_database

function fetch_all_data()
    E_data = DataFetch.fetch_exposure_data(2021, 2021)
    # Path to the DuckDB database file
    db_path = "data/raw/DatAdapt_1990-2021.duckdb"

    
    #climate_data = DataFetch.fetch_hazard_data()
    #disaster_data = DataFetch.fetch_damage_data()
    #combined_data = vcat(gdp_data, climate_data, disaster_data)
    return combined_data
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
