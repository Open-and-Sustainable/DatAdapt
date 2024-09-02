module HazardDataFetch

using HTTP
using Tar
using DataFrames
using CSV
using CodecZlib
using Dates
using NCDatasets
using PyCall
using Shapefile, LibGEOS, GeoDataFrames

export fetch_hazard_data


function fetch_hazard_data(start_year::Int, end_year::Int)
    data = fetch_era5_data(start_year, end_year)
    return data
end

function fetch_era5_data(start_year::Int, end_year::Int)
    destination_dir = "DatAdapt-database/raw/era5/"
    mkpath(destination_dir)
    
    # Define the years and months you want to download
    years = string.(start_year:end_year)
    months = ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"]
    
    dataset = "reanalysis-era5-single-levels"

    # Import the cdsapi Python module and create the client inside the function
    cdsc = pyimport("cdsapi")
    c = cdsc.Client()

    # Loop through each year and month to download data
    for year in years
        for month in months
            output_file = joinpath(destination_dir, "era5_data_$year-$month.nc")
            
            if isfile(output_file)
                println("File $output_file already exists. Skipping download.")
            else
                println("Downloading ERA5 data for $year-$month...")

                # Define the parameters as a Julia Dict, which will be passed to Python
                # Temperature is measured in Kelvin, subtract 273.15 to convert to Celsius
                # Total precipitation is measured in m, rain and snow
                # Wind speed is measured in m/s, Instantaneous 10 metre wind gust (10 meters height)
                # Potential evaporation is measured in m
                request_params = Dict(
                    "product_type" => "reanalysis",
                    "variable" => [
                        "2m_temperature", "total_precipitation", 
                        "instantaneous_10m_wind_gust", 
                        "potential_evaporation"
                    ],
                    "year" => year,
                    "month" => month,
                    "day" => ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
                              "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
                              "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"],
                    "time" => ["00:00", "01:00", "02:00", "03:00", "04:00", "05:00", "06:00", "07:00",
                               "08:00", "09:00", "10:00", "11:00", "12:00", "13:00", "14:00", "15:00",
                               "16:00", "17:00", "18:00", "19:00", "20:00", "21:00", "22:00", "23:00"],
                    "format" => "netcdf",
                    "download_format" => "unarchived"
                )

                # Retrieve the data using the client
                result = c.retrieve(dataset, request_params)
                result.download(output_file)

                println("Download complete: $output_file")

                # Process the NetCDF file into a DataFrame
                # Load the NetCDF file
                dataset = Dataset(output_file)

                # Extract variables
                t2m = dataset["t2m"][:]  # 2 meter temperature
                tp = dataset["tp"][:]    # Total precipitation
                i10fg = dataset["i10fg"][:]  # Instantaneous 10m wind gust
                pev = dataset["pev"][:]   # Potential evaporation
                latitude = dataset["latitude"][:]
                longitude = dataset["longitude"][:]
                valid_time = dataset["valid_time"][:]  # Time variable

                # Convert time to DateTime format
                times = DateTime.(valid_time, Dates.DateFormat("yyyy-mm-ddTHH:MM:SS"))

                # Group data by day
                days = unique(Date.(times))

                # Initialize arrays to hold daily values
                daily_min_temp = fill(NaN, length(latitude), length(longitude), length(days))
                daily_max_temp = fill(NaN, length(latitude), length(longitude), length(days))
                daily_avg_evap = fill(NaN, length(latitude), length(longitude), length(days))
                daily_max_gusts = fill(NaN, length(latitude), length(longitude), length(days))

                # Loop over each day and calculate daily statistics
                for day_idx in eachindex(days)
                    day = days[day_idx]
                    day_mask = Date.(times) .== day

                    for i in axes(latitude, 1)
                        for j in axes(longitude, 1)
                            daily_min_temp[i, j, day_idx] = minimum(t2m[i, j, day_mask])
                            daily_max_temp[i, j, day_idx] = maximum(t2m[i, j, day_mask])
                            daily_avg_evap[i, j, day_idx] = mean(pev[i, j, day_mask])
                            daily_max_gusts[i, j, day_idx] = maximum(i10fg[i, j, day_mask])
                        end
                    end
                end

                # Load shapefile with country boundaries
                shapefile = Shapefile.Table("DatAdapt-database/raw/World_Countries.shp")

                # Create a GEOS context for point-in-polygon operations
                context = LibGEOS.Context()

                # Initialize matrix to store country assignments
                country_assignment = fill("", length(latitude), length(longitude))

                # Assign grid points to countries using axes for indexing
                for i in axes(latitude, 1)
                    for j in axes(longitude, 1)
                        point = Point(longitude[j], latitude[i])
                        for feature in shapefile
                            if intersects(point, feature.geometry)
                                country_assignment[i, j] = feature.properties["ISO_CC"]
                                break
                            end
                        end
                    end
                end

                sea_mask = country_assignment .== ""

                # Apply the mask to filter out sea points
                filtered_min_temp = daily_min_temp[!sea_mask]
                filtered_max_temp = daily_max_temp[!sea_mask]
                filtered_avg_evap = daily_avg_evap[!sea_mask]
                filtered_max_gusts = daily_max_gusts[!sea_mask]
                filtered_countries = country_assignment[!sea_mask]

                df = DataFrame(
                    Date = repeat(days, outer=[length(filtered_countries)]),
                    Country = filtered_countries,
                    MinTemperature = filtered_min_temp,
                    MaxTemperature = filtered_max_temp,
                    AvgEvapotranspiration = filtered_avg_evap,
                    MaxGusts = filtered_max_gusts
                )

                # Save to a CSV or another suitable format
                csv_file = joinpath(destination_dir, "era5_data_$year-$month.csv")
                CSV.write(csv_file, df)

                # close access to dataset
                close(dataset)
                # remove downloaded file
            end
        end
    end
    return DataFrame()
end

function process_nc_file(file::String)
    # Load the NetCDF file and process it into a DataFrame
    ds = Dataset(file)
    varnames = keys(ds)
    df = DataFrame()
    
    for varname in varnames
        var = ds[varname]
        if ndims(var) == 2  # Assuming 2D data for simplicity
            df[!, Symbol(varname)] = vec(var[:])
        elseif ndims(var) == 3  # Handle 3D data
            df[!, Symbol(varname)] = vec(var[:, :, 1])  # Modify according to your needs
        end
    end
    
    close(ds)
    return df
end


end