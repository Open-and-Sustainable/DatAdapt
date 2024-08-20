#!/bin/bash
# Set up environment variables for LFS
# Usage: ./setup-lfs.sh <your-specific-project-token>

# Check if the token was passed as a parameter
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <LFS_TOKEN>"
    exit 1
fi

# Assign the passed token to a variable
LFS_TOKEN=$1

# Define the LFS URL
GIT_LFS_URL="https://git.nilu.no/ribo/open-and-sustainable-lfs.git/info/lfs"

# Export the LFS URL as an environment variable (optional, depending on use case)
export GIT_LFS_URL

# Configure git to use the LFS URL and token
git config --local lfs.url "$GIT_LFS_URL"
git config --local http.extraHeader "Authorization: Bearer $LFS_TOKEN"

