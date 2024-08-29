module DatabaseAccess

using DuckDB
using DataFrames
using Dates
using CSV

export write_duckdb_table, write_large_duckdb_table, executePRQL

function escape_sql_string(value::String)::String
    # Manually escape single quotes by doubling them
    escaped_value = "'"
    for char in value
        if char == '\''
            escaped_value *= "''"
        else
            escaped_value *= string(char)
        end
    end
    escaped_value *= "'"
    return escaped_value
end

function sql_value(value)::String
    # Convert a Julia value to a proper SQL representation
    if value === nothing || value === missing
        return "NULL"
    elseif value isa String
        return escape_sql_string(value)
    else
        return string(value)
    end
end

function create_and_load_table_throughCSV!(df::DataFrame, con::DuckDB.DB, table_name::String)
    # Drop the table if it exists
    DBInterface.execute(con, "DROP TABLE IF EXISTS $table_name")

    # Create the table with explicit types
    create_table_with_types!(df, con, table_name)

    # Write DataFrame to a temporary CSV file
    temp_csv_path = "DatAdapt-database/raw/temp_data.csv"
    CSV.write(temp_csv_path, df)

    # Load data from the CSV file using COPY
    DBInterface.execute(con, "COPY $table_name FROM '$temp_csv_path' (FORMAT CSV, HEADER TRUE)")

    # Remove the temporary CSV file
    rm(temp_csv_path)
end

function create_and_load_table_directly!(df::DataFrame, con::DuckDB.DB, table_name::String)
    # Drop the table if it exists
    DBInterface.execute(con, "DROP TABLE IF EXISTS $table_name")

    # Create the table with explicit types
    create_table_with_types!(df, con, table_name)

    # Determine the column names and types
    column_names = names(df)
    column_types = eltype.(eachcol(df))

    # Insert data into the table
    for row in eachrow(df)
        values_sql = String[]
        for (name, _) in zip(column_names, column_types)
            value = row[name]
            push!(values_sql, sql_value(value))
        end
        insert_sql = "INSERT INTO $table_name VALUES ($(join(values_sql, ", ")))"
        DBInterface.execute(con, insert_sql)
    end

end

function write_duckdb_table!(df::DataFrame, db_path::String, table_name::String)
    # Create or connect to the DuckDB database
    con = DuckDB.DB(db_path)
    
    # Create the table with types and load data
    create_and_load_table_directly!(df, con, table_name)
    
    # Close the connection
    DBInterface.close!(con)
end

function write_large_duckdb_table!(df::DataFrame, db_path::String, table_name::String)
    # Create or connect to the DuckDB database
    con = DuckDB.DB(db_path)
    
    # Create the table with types and load data
    create_and_load_table_throughCSV!(df, con, table_name)
    
    # Close the connection
    DBInterface.close!(con)
end

function create_table_with_types!(df::DataFrame, con::DuckDB.DB, table_name::String)
    # Determine the column names and types
    column_names = names(df)
    column_types = eltype.(eachcol(df))

    # Map Julia types to SQL types
    type_map = Dict(
        Int => "INTEGER",
        Float64 => "DOUBLE",
        String => "STRING",
        Bool => "BOOLEAN",
        Dates.Date => "DATE",
        Dates.DateTime => "TIMESTAMP"
        # Add more mappings as needed
    )

    # Construct the CREATE TABLE statement with quoted column names and SQL types
    columns_sql = String[]
    for (name, col_type) in zip(column_names, column_types)
        quoted_name = "\"" * name * "\""  # Quote the column name
        sql_type = get(type_map, col_type, "STRING")  # Default to STRING if type is not mapped
        push!(columns_sql, "$quoted_name $sql_type")
    end
    create_table_sql = "CREATE TABLE $table_name ($(join(columns_sql, ", ")))"
    DBInterface.execute(con, create_table_sql)
end

function installPRQL_DuckDBextension()
    con = DuckDB.DB()
    try
        # Attempt to install the PRQL extension
        DuckDB.execute(con, "INSTALL 'prql' FROM community;")
        DuckDB.execute(con, "LOAD 'prql';")
        
        println("PRQL extension installed and loaded successfully.")
    catch e
        println("Error during PRQL extension installation: ", e)
    finally
        DBInterface.close!(con)
    end
end

function executePRQL(dbpath::String, prqlpath::String)::DataFrame
    # Create a connection to the DuckDB database
    con = DuckDB.DB(dbpath)
    
    try
        # Load the PRQL extension
        DuckDB.execute(con, "LOAD 'prql';")
        
        # Read the PRQL code from the file
        prql_query = read(prqlpath, String)
        
        # Execute the PRQL query and capture the result
        result_df = DataFrame(DuckDB.query(con, prql_query))
        
        # Return the resulting DataFrame
        return result_df
    catch e
        # Handle any errors that occur during the process
        println("Error during execution: ", e)
        return DataFrame()  # Return an empty DataFrame in case of error
    finally
        # Ensure the database connection is closed
        DBInterface.close!(con)
    end
end


end # module DatabaseAccess