#!/bin/bash

# This script runs integration tests on the pg_sparse extension using pg_regress. To add new tests, add
# a new .sql file to the test/sql directory and add the corresponding .out file to the test/expected
# directory, and it will automatically get executed by this script.

# Exit on subcommand errors
set -Eeuo pipefail

# Handle params
usage() {
  echo "Usage: $0 [OPTIONS]"
  echo "Options:"
  echo " -h (optional),   Display this help message"
  echo " -p (required),   Processing type, either <sequential> or <threaded>"
  echo " -v (optional),   PG version(s) separated by comma <12,13,14>"
  exit 1
}

# Do not allow script to be called without params
if [[ ! $* =~ ^\-.+ ]]
then
  usage
fi

# Instantiate vars
FLAG_PG_VER=false
FLAG_PROCESS_TYPE=false

# Assign flags to vars and check
while getopts "hp:v:u:" flag
do
  case $flag in
    h)
      usage
      ;;
    p)
      FLAG_PROCESS_TYPE=$OPTARG
    case "$FLAG_PROCESS_TYPE" in sequential | threaded ): # Do nothing
          ;;
        *)
          usage
          ;;
      esac
      ;;
    v)
      FLAG_PG_VER=$OPTARG
      ;;
    *)
      usage
      ;;
  esac
done

# Determine the base directory of the script
BASEDIR=$(dirname "$0")
cd "$BASEDIR/../"
BASEDIR=$(pwd)

# Vars
OS_NAME=$(uname)
export PGUSER=postgres
export PGDATABASE=postgres
export PGPASSWORD=password

# All pgrx-supported PostgreSQL versions to configure for
OS_NAME=$(uname)
if [ "$FLAG_PG_VER" = false ]; then
  # No arguments provided; use default versions
  PG_VERSIONS=("16" "15" "14" "13" "12")
else
  IFS=',' read -ra PG_VERSIONS <<< "$FLAG_PG_VER"  # Split the argument by comma into an array
fi


# Cleanup function
cleanup() {
  # Check if regression.diffs exists and print if present
  if [ -f "$BASEDIR/regression.diffs" ]; then
    echo "Some test(s) failed! Printing the diff between the expected and actual test results..."
    cat "$BASEDIR/regression.diffs"
  fi
  echo "Cleaning up..."

  # Clean up the test database and temporary files
  rm -rf "$PWFILE"
  rm -rf "$TMPDIR"
  rm -rf "$BASEDIR/test/test_logs.log"
  rm -rf "$BASEDIR/regression.diffs"
  rm -rf "$BASEDIR/regression.out"
  echo "Done, goodbye!"
}

# Register the cleanup function to run when the script exits
trap cleanup EXIT


function run_tests() {
  TMPDIR="$(mktemp -d)"
  export PGDATA="$TMPDIR"
  export PGHOST="$TMPDIR"

  # Create a temporary password file
  PWFILE=$(mktemp)
  echo "$PGPASSWORD" > "$PWFILE"

  # Ensure a clean environment
  unset TESTS

  # Initialize the test database
  echo "Initializing PostgreSQL test database..."
  initdb --no-locale --encoding=UTF8 --nosync -U "$PGUSER" --auth-local=md5 --auth-host=md5 --pwfile="$PWFILE" > /dev/null
  pg_ctl start -o "-F -c listen_addresses=\"\" -c log_min_messages=WARNING -k $PGDATA" > /dev/null
  createdb test_db

  # Set PostgreSQL logging configuration
  echo "Setting test database logging configuration..."
  psql -v ON_ERROR_STOP=1 -c "ALTER SYSTEM SET logging_collector TO 'on';" -d test_db > /dev/null
  psql -v ON_ERROR_STOP=1 -c "ALTER SYSTEM SET log_directory TO '$BASEDIR/test/';" -d test_db > /dev/null
  psql -v ON_ERROR_STOP=1 -c "ALTER SYSTEM SET log_filename TO 'test_logs.log';" -d test_db > /dev/null

  # Configure search_path to include the paradedb schema
  psql -v ON_ERROR_STOP=1 -c "ALTER USER $PGUSER SET search_path TO public,paradedb;" -d test_db > /dev/null

  # Reload PostgreSQL configuration
  echo "Reloading PostgreSQL configuration..."
  pg_ctl restart > /dev/null

  # Build and install the extension
  echo "Building and installing pg_sparse..."
  case "$OS_NAME" in
    Darwin)
      make clean
      # Check arch to set proper pg_config path
      if [ "$(uname -m)" = "arm64" ]; then
        make PG_CONFIG="/opt/homebrew/opt/postgresql@$PG_VERSION/bin/pg_config"
        make install PG_CONFIG="/opt/homebrew/opt/postgresql@$PG_VERSION/bin/pg_config"
      elif [ "$(uname -m)" = "x86_64" ]; then
        make PG_CONFIG="/usr/local/opt/postgresql@$PG_VERSION/bin/pg_config"
        make install PG_CONFIG="/usr/local/opt/postgresql@$PG_VERSION/bin/pg_config"
      else
        echo "Unknown arch, exiting..."
        exit 1
      fi
      ;;
    Linux)
      sudo make clean
      sudo PG_CONFIG="/usr/lib/postgresql/$PG_VERSION/bin/pg_config" make
      sudo PG_CONFIG="/usr/lib/postgresql/$PG_VERSION/bin/pg_config" make install
      ;;
  esac

  # Execute tests using pg_regress
  # We always test on the upcoming version, which means that this test also acts as an extension upgrade test
  echo "Running tests..."
  make installcheck
}

# Loop over PostgreSQL versions
for PG_VERSION in "${PG_VERSIONS[@]}"; do
  echo ""
  echo "***********************************************************"
  echo "* Running tests ($FLAG_PROCESS_TYPE) for PostgreSQL version: $PG_VERSION"
  echo "***********************************************************"
  echo ""

  # Install the specific PostgreSQL version if it's not already installed
  case "$OS_NAME" in
    Darwin)
      brew install postgresql@"$PG_VERSION" > /dev/null 2>&1
      ;;
    Linux)
      sudo apt-get install -y "postgresql-$PG_VERSION" "postgresql-server-dev-$PG_VERSION" > /dev/null 2>&1
      ;;
  esac

  if [ "$FLAG_PROCESS_TYPE" = "threaded" ]; then
    run_tests &
  else
    run_tests
  fi
done

# Wait for all child processes to finish
wait
