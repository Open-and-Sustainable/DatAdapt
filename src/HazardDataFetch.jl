module HazardDataFetch

using HTTP
using Tar
using DataFrames
using CSV
using CodecZlib
using Dates
using CDSAPI
using NCDatasets

export fetch_hazard_data


function fetch_hazard_data_OLD(start_year::Int, end_year::Int)
    destination_dir = "data/raw/nathaz/"
    extraction_dir = joinpath(destination_dir, "extracted")
    mkpath(destination_dir)
    
    url = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/archive/daily-summaries-latest.tar.gz"
    tarball_path = joinpath(destination_dir, "daily-summaries-latest.tar.gz")

    if isfile(tarball_path)
        println("File already exists. Skipping download.")
    else
        println("Downloading data...")
        HTTP.download(url, tarball_path)
        println("Download complete.")
    end

    # Check if files are already extracted
    if isdir(extraction_dir) && length(readdir(extraction_dir)) > 0
        println("Files already extracted. Skipping decompression.")
    else
        # Ensure the extraction directory is empty
        if isdir(extraction_dir)
            rm(extraction_dir; force = true, recursive = true)
        end
        mkpath(extraction_dir)

        # Decompress and extract the tar.gz file
        println("Decompressing and extracting data...")
        open(tarball_path) do tar_gz
            tar = GzipDecompressorStream(tar_gz)
            Tar.extract(tar, extraction_dir)
            close(tar)
        end
        println("Decompression and extraction complete.")
    end

    # Process the files in batches
    println("Loading data from extracted files...")
    combined_df = DataFrame()
    files = readdir(extraction_dir, join=true)

    batch_size = 10000
    num_batches = div(length(files), batch_size) + 1

    for batch_num in 1:num_batches
        start_idx = (batch_num - 1) * batch_size + 1
        end_idx = min(batch_num * batch_size, length(files))
        files_batch = files[start_idx:end_idx]

        println("Processing batch $batch_num of $num_batches...")
        data_frames = process_file_batch(files_batch, start_year, end_year)

        if !isempty(data_frames)
            align_columns!(data_frames)
            batch_combined_df = vcat(data_frames...; cols=:union)
            combined_df = vcat(combined_df, batch_combined_df; cols=:union)
        end

        println("After batch $batch_num: Combined DataFrame has $(nrow(combined_df)) rows and $(ncol(combined_df)) columns.")
    end

    if !isempty(combined_df)
        return combined_df
    else
        println("No data to combine.")
        return DataFrame()
    end
end

function process_file_batch(files::Vector{String}, start_year::Int, end_year::Int)
    data_frames = DataFrame[]

    for file in files
        if endswith(file, ".csv")
            println("Processing $file...")
            try
                df = CSV.read(file, DataFrame)
                
                if "DATE" in names(df)
                    filter!(row -> start_year <= year(row["DATE"]) <= end_year, df)
                    dropmissing!(df)

                    if nrow(df) > 0
                        push!(data_frames, df)
                    else
                        println("DataFrame from $file is empty after filtering.")
                    end
                else
                    println("Skipping $file: 'DATE' column not found.")
                end
            catch e
                println("Error processing $file: $e")
            end
        end
    end
    
    return data_frames
end

function align_columns!(dfs::Vector{DataFrame})
    # Collect all unique columns across all DataFrames
    all_columns = unique(vcat([names(df) for df in dfs]...))

    for (i, df) in enumerate(dfs)
        # Add missing columns if needed       
        for col in all_columns
            if !(col in names(df))
                df[!, Symbol(col)] = fill(missing, nrow(df))
            end
        end
    end
end


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
    
    # Loop through each year and month to download data
    for year in years
        for month in months
            output_file = joinpath(destination_dir, "era5_data_$year-$month.nc")
            
            if isfile(output_file)
                println("File $output_file already exists. Skipping download.")
            else
                println("Downloading ERA5 data for $year-$month...")
                
                json_string = """
                {
                    "product_type": "reanalysis",
                    "variable": [
                        "2m_temperature", "total_precipitation", 'instantaneous_10m_wind_gust'
                    ],
                    "year": "$year",
                    "month": "$month",
                    "day": [
                        "01", "02", "03", "04", "05", "06", "07", "08", "09", "10",
                        "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
                        "21", "22", "23", "24", "25", "26", "27", "28", "29", "30", "31"
                    ],
                    "time": [
                        "00:00", "01:00", "02:00", "03:00", "04:00", "05:00", "06:00", "07:00",
                        "08:00", "09:00", "10:00", "11:00", "12:00", "13:00", "14:00", "15:00",
                        "16:00", "17:00", "18:00", "19:00", "20:00", "21:00", "22:00", "23:00"
                    ],
                    "format": "netcdf",
                    'download_format': 'unarchived'
                }
                """
                #println(json_string)  # Print the JSON string to check formatting
                CDSAPI.retrieve("reanalysis-era5-single-levels", CDSAPI.py2ju(json_string), output_file)

                println("Download complete: $output_file")
            end
        end
    end
    
    # Process the downloaded files
    println("Processing downloaded ERA5 data...")
    combined_df = DataFrame()
    files = readdir(destination_dir, join=true)

    for file in files
        if endswith(file, ".nc")
            println("Processing $file...")
            df = process_nc_file(file)
            combined_df = vcat(combined_df, df; cols=:union)
        end
    end
    
    return combined_df
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