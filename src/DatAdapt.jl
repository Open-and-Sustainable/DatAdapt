module DatAdapt

using DataFrames

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
    gdp_data = DataFetch.fetch_economic_data()
    climate_data = DataFetch.fetch_climate_data()
    disaster_data = DataFetch.fetch_disaster_data()
    combined_data = vcat(gdp_data, climate_data, disaster_data)
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
