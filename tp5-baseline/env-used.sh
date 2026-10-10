umask 077
root="${TP5_ROOT:-/Users/roxor/rq-tp5-prc}"
mkdir -p "${root}/tmp" "${root}/data" "${root}/parquet" "${root}/logs"
chmod 700 "${root}" "${root}/tmp" "${root}/data" "${root}/parquet" "${root}/logs"
export TMPDIR="${root}/tmp" TMP="${root}/tmp" TEMP="${root}/tmp"
export DATA_DIR="${root}/data"
export DUCKDB_PATH="${root}/data/test.duckdb"
export DUCKDB_READONLY_PATH="${root}/data/test-ro.duckdb"
export PARQUET_DIR="${root}/parquet"
export LOG_DIR="${root}/logs"
export RQUANT_DISABLE_DOTENV=1
export TUSHARE_TOKEN_MAIN=00000000000000000000000000000000
export NOTIFY_ENABLED=false
