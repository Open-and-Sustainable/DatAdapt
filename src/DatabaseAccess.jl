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

function create_and_insert_table!(df::DataFrame, con::DuckDB.DB, table_name::String)
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

    # Construct the CREATE TABLE statement
    columns_sql = String[]
    for (name, col_type) in zip(column_names, column_types)
        sql_type = get(type_map, col_type, "STRING")  # Default to STRING if type is not mapped
        push!(columns_sql, "$name $sql_type")
    end
    create_table_sql = "CREATE TABLE IF NOT EXISTS $table_name ($(join(columns_sql, ", ")))"
    DBInterface.execute(con, create_table_sql)

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

function write_duckdb_table(df::DataFrame, db_path::String, table_name::String)
    # Create or connect to the DuckDB database
    con = create_or_connect_duckdb(db_path)
    # Drop the table if it exists
    DBInterface.execute(con, "DROP TABLE IF EXISTS $table_name")
    # create a new one with the DataFrame data
    create_and_insert_table!(df, con, table_name)
    # Close the connection
    DBInterface.close!(con)
end

end # module DatabaseAccess