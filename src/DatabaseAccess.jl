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

function create_and_load_table!(df::DataFrame, con::DuckDB.DB, table_name::String)
    # Drop the table if it exists
    DBInterface.execute(con, "DROP TABLE IF EXISTS $table_name")

    # Create the table based on DataFrame schema
    DBInterface.execute(con, "CREATE TABLE $table_name AS SELECT * FROM df LIMIT 0")

    # Bulk load the DataFrame into the table
    DBInterface.load!(df, con, table_name)
end

function write_duckdb_table!(df::DataFrame, db_path::String, table_name::String)
    # Create or connect to the DuckDB database
    con = DuckDB.DB(db_path)
    
    # Create the table and load data
    create_and_load_table!(df, con, table_name)
    
    # Close the connection
    DBInterface.close!(con)
end

end # module DatabaseAccess