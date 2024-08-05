module DatabaseAccess

using DuckDB
using DataFrames

function create_or_connect_duckdb(db_path::String)
    # Open a DuckDB connection (this creates the database if it doesn't exist)
    return DBInterface.connect(DuckDB.DB, db_path)
end

function append_to_duckdb_table(df::DataFrame, con, table_name::String)
    # Check if the table exists
    existing_tables = DBInterface.execute(con, "SHOW TABLES").value
    if !(table_name in existing_tables)
        # If the table does not exist, create it
        DBInterface.execute(con, "CREATE TABLE $table_name AS SELECT * FROM df WHERE 0=1")
    end
    
    # Insert data from the DataFrame into the table
    DBInterface.execute(con, "INSERT INTO $table_name SELECT * FROM df")
end

function overwrite_duckdb_table(df::DataFrame, con, table_name::String)
    # Drop the table if it exists and create a new one with the DataFrame data
    DBInterface.execute(con, "CREATE OR REPLACE TABLE $table_name AS SELECT * FROM df")
end

function append_table(df::DataFrame, con::, table_name::String)
    # Create or connect to the DuckDB database
    con = create_or_connect_duckdb(db_path)
    # Save or append to tables in the DuckDB database
    append_to_duckdb_table(df1, con, "example_table")

end # module DatabaseAccess