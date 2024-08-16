module HazardDataFetch

using HTTP
using Tar
using DataFrames
using CSV
using SHA
using Base64

using Base.Filesystem: mkpath

export fetch_hazard_data


function fetch_hazard_data(start_year::Int, end_year::Int)
    destination_dir = "data/raw/nathaz/"
    mkpath(destination_dir)
    
    url = "https://www.ncei.noaa.gov/data/global-historical-climatology-network-daily/archive/daily-summaries-latest.tar.gz"
    tarball_path = joinpath(destination_dir, "daily-summaries-latest.tar.gz")
    
    # Compute the online checksum
    println("Computing online checksum...")
    online_checksum = compute_online_checksum(url)
    
    if isfile(tarball_path)
        println("File already exists. Verifying checksum...")
        
        file_checksum = open(tarball_path, "r") do file
            bytes2hex(sha256(file))
        end
        
        if file_checksum == online_checksum
            println("Checksum matches. Skipping download.")
        else
            println("Checksum does not match. Re-downloading file.")
            open(tarball_path, "w") do file
                HTTP.download(url, file)
            end
        end
    else
        println("Downloading data...")
        open(tarball_path, "w") do file
            HTTP.download(url, file)
        end
        println("Download complete.")
    end

    # Extract the tarball
    println("Extracting data...")
    untar(tarball_path, destination_dir)
    println("Extraction complete.")

    # Load data from the extracted files
    println("Loading data from extracted files...")
    data_frames = DataFrame[]
    files = readdir(destination_dir, join=true)
    
    for file in files
        if endswith(file, ".csv")
            println("Processing $file...")
            df = CSV.read(file, DataFrame; dateformat="yyyy-mm-dd")
            
            # Ensure the DATE column is treated as a Date object
            df.DATE = Date.(df.DATE, "yyyy-mm-dd")
            
            # Filter for observations within the specified date range
            filter!(row -> year(row.DATE) >= start_year && year(row.DATE) <= end_year, df)
            
            # If the DataFrame has data, add it to the list
            if nrow(df) > 0
                push!(data_frames, df)
            end
        end
    end
    
    # Combine all DataFrames into one
    combined_df = vcat(data_frames...)
    println("Data loading complete. Combined DataFrame has $(nrow(combined_df)) rows.")
    
    return combined_df
end

function compute_online_checksum(url::String, algorithm::Function = sha256)
    # Initialize the hash object
    ctx = algorithm()

    # Stream the file content from the URL
    HTTP.open("GET", url) do io
        while !eof(io)
            buffer = read(io, 1024 * 1024)  # Read in 1 MB chunks
            update!(ctx, buffer)
        end
    end

    # Finalize and return the checksum as a hex string
    return bytes2hex(ctx)
end

end
