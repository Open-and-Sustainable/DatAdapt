module HazardDataFetch

using HTTP
using Tar
using DataFrames
using CSV
using CodecZlib
using Dates

export fetch_hazard_data


function fetch_hazard_data(start_year::Int, end_year::Int)
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



end