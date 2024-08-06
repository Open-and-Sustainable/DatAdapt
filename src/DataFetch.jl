module DataFetch

using HTTP
using JSON
using DataFrames

export fetch_exposure_data

function fetch_exposure_data(start_year::Int, end_year::Int)
    base_url = "http://api.worldbank.org/v2/country/all/indicator/"
    indicators = [
        "SP.POP.TOTL",         # Population, total
        "AG.SRF.TOTL.K2",      # Surface area (sq. km)
        "EN.POP.DNST",         # Population density (people per sq. km of land area)
        "NY.GDP.MKTP.CD",      # GDP (current US$)
        "NY.GDP.MKTP.KD.ZG",   # GDP growth (annual %)
        "NY.GNP.PCAP.CD",      # GNI per capita, Atlas method (current US$)
        "NY.GNP.PCAP.PP.CD"    # GNI per capita, PPP (current international $)
    ]

    data_frames = DataFrame[]

    for indicator in indicators
        page = 1
        total_pages = 1

        while page <= total_pages
            url = string(base_url, indicator, "?date=", start_year, ":", end_year, "&format=json&per_page=50&page=", page)

            # Making the API request
            response = HTTP.get(url)
            if response.status != 200
                error("Failed to fetch data for indicator $indicator: HTTP status $(response.status)")
            end

            # Parsing the response body
            data = JSON.parse(String(response.body))

            # Set total_pages based on the first page response
            if page == 1
                total_pages = data[1]["pages"]
            end

            records = data[2]  # Data is in the second element of the list

            # Extracting relevant fields and converting to DataFrame
            df = DataFrame(
                country_name = [record["country"]["value"] for record in records],
                country_code = [record["country"]["id"] for record in records],
                year = [record["date"] for record in records],
                value = [record["value"] for record in records]
            )
            df.indicator = fill(indicator, nrow(df))
            push!(data_frames, df)

            page += 1
        end
    end

    # Combine all data frames into one
    combined_df = vcat(data_frames...)
    return combined_df
end


function fetch_WB_test_data()
    base_url = "http://api.worldbank.org/v2/country/all/indicator/"
    indicators = [
        "SP.POP.TOTL"         # Population, total
    ]

    data_frames = DataFrame[]

    for indicator in indicators
        page = 1
        total_pages = 1

        while page <= total_pages
            url = string(base_url, indicator, "?date=2021:2021&format=json&per_page=50&page=", page)

            # Making the API request
            response = HTTP.get(url)
            if response.status != 200
                error("Failed to fetch data for indicator $indicator: HTTP status $(response.status)")
            end

            # Parsing the response body
            data = JSON.parse(String(response.body))

            # Set total_pages based on the first page response
            if page == 1
                total_pages = data[1]["pages"]
            end

            records = data[2]  # Data is in the second element of the list

            # Extracting relevant fields and converting to DataFrame
            df = DataFrame(
                country_name = [record["country"]["value"] for record in records],
                country_code = [record["country"]["id"] for record in records],
                year = [record["date"] for record in records],
                value = [record["value"] for record in records]
            )
            df.indicator = fill(indicator, nrow(df))
            push!(data_frames, df)

            page += 1
        end
    end

    # Combine all data frames into one
    combined_df = vcat(data_frames...)
    return combined_df
end

function fetch_hazard_data()
    # Placeholder for actual implementation
    # You would replace this with actual API requests or data fetching logic
    df = DataFrame()
    return df
end

function fetch_damage_data()
    # Placeholder for actual implementation
    df = DataFrame()
    return df
end

end # module DataFetch
