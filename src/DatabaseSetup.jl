module DatabaseSetup

using SQLite
using DataFrames

"""
    setup_database(db_path::String)

Creates and sets up the SQLite database schema.
"""
function setup_database(db_path::String)
    # Connect to the SQLite database (creates if not exists)
    db = SQLite.DB(db_path)

    # Example: Create a table for GDP data
    SQLite.execute(db, """
    CREATE TABLE IF NOT EXISTS gdp_data (
        country TEXT,
        year INT,
        gdp REAL
    )
    """)

    # Example: Create a table for climate data
    SQLite.execute(db, """
    CREATE TABLE IF NOT EXISTS climate_data (
        parameter TEXT,
        year INT,
        value REAL
    )
    """)

    # Example: Create a table for disaster data
    SQLite.execute(db, """
    CREATE TABLE IF NOT EXISTS disaster_data (
        event_id TEXT,
        event_type TEXT,
        country TEXT,
        year INT,
        damage REAL
    )
    """)

    # Close the database connection
    SQLite.close(db)
end

"""
    insert_gdp_data!(db_path::String, df::DataFrame)

Inserts cleaned GDP data into the database.
"""
function insert_gdp_data!(db_path::String, df::DataFrame)
    db = SQLite.DB(db_path)

    # Insert data into the gdp_data table
    for row in eachrow(df)
        SQLite.execute(db, """
        INSERT INTO gdp_data (country, year, gdp) VALUES (?, ?, ?)
        """, (row[:country], row[:year], row[:gdp]))
    end

    SQLite.close(db)
end

"""
    insert_climate_data!(db_path::String, df::DataFrame)

Inserts cleaned climate data into the database.
"""
function insert_climate_data!(db_path::String, df::DataFrame)
    db = SQLite.DB(db_path)

    # Implement the insertion logic for climate data
    for row in eachrow(df)
        # Example insert statement
        SQLite.execute(db, """
        INSERT INTO climate_data (parameter, year, value) VALUES (?, ?, ?)
        """, (row[:parameter], row[:year], row[:value]))
    end

    SQLite.close(db)
end

"""
    insert_disaster_data!(db_path::String, df::DataFrame)

Inserts cleaned disaster data into the database.
"""
function insert_disaster_data!(db_path::String, df::DataFrame)
    db = SQLite.DB(db_path)

    # Implement the insertion logic for disaster data
    for row in eachrow(df)
        SQLite.execute(db, """
        INSERT INTO disaster_data (event_id, event_type, country, year, damage) VALUES (?, ?, ?, ?, ?)
        """, (row[:event_id], row[:event_type], row[:country], row[:year], row[:damage]))
    end

    SQLite.close(db)
end

end # module DatabaseSetup
