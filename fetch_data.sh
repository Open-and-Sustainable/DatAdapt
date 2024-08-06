#!/bin/bash

# Run the Julia script and call the fetch_exposure_data function
julia --project=. -e 'push!(LOAD_PATH, "src"); using DatAdapt; DatAdapt.fetch_exposure_data()'