module DatabaseAccess

using DuckDB
using DataFrames
using Dates

export write_duckdb_table

function create_or_connect_duckdb(db_path::String)
    # Open a DuckDB connection (this creates the database if it doesn't exist)
    return DBInterface.connect(DuckDB.DB, db_path)
end

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

function create_and_load_table_with_copy!(df::DataFrame, con::DuckDB.DB, table_name::String)
    # Drop the table if it exists
    DBInterface.execute(con, "DROP TABLE IF EXISTS $table_name")

    # Create the table with explicit types
    create_table_with_types!(df, con, table_name)

    # Write DataFrame to a temporary CSV file
    temp_csv_path = "data/raw/temp_data.csv"
    CSV.write(temp_csv_path, df)

    # Load data from the CSV file using COPY
    DBInterface.execute(con, "COPY $table_name FROM '$temp_csv_path' (FORMAT CSV, HEADER TRUE)")

    # Remove the temporary CSV file
    rm(temp_csv_path)
end

function write_duckdb_table!(df::DataFrame, db_path::String, table_name::String)
    # Create or connect to the DuckDB database
    con = DuckDB.DB(db_path)
    
    # Create the table with types and load data
    create_and_load_table!(df, con, table_name)
    
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

end # module DatabaseAccess