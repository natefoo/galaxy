#!/bin/sh
set -e

usage="usage: $0 <root-dir>"

if [ -z "$1" ]; then
    echo "$usage"
    exit 1;
else
    deploy_root="$1"
fi


cd "$(dirname "$0")"

. ./scripts/common_startup_functions.sh

for dir in bin config log data; do
    mkdir -p "${deploy_root}/${dir}"
done

: ${GALAXY_VIRTUAL_ENV:="${deploy_root}/data/venv"}
: ${GALAXY_CONFIG_FILE:="${deploy_root}/config/galaxy.yml"}

export GALAXY_VIRTUAL_ENV GALAXY_CONFIG_FILE

parse_common_args $@

run_common_start_up

setup_python

set_galaxy_config_file_var

if [ "$INITIALIZE_TOOL_DEPENDENCIES" -eq 1 ]; then
    # Install Conda environment if needed.
    python ./scripts/manage_tool_dependencies.py init_if_needed
fi

if [ ! -f "$GALAXY_CONFIG_FILE" ]; then
    echo "Initializing $GALAXY_CONFIG_FILE from config/galaxy.yml.sample"
    sed -e "s%^  #config_dir: null$%  config_dir: ${deploy_root}/config%" \
        -e "s%^  #managed_config_dir: null$%  managed_config_dir: ${deploy_root}/data/config%" \
        -e "s%^  #data_dir: null$%  data_dir: ${deploy_root}/data%" \
        config/galaxy.yml.sample > "$GALAXY_CONFIG_FILE"
fi

if [ ! -f "${deploy_root}/config/local_env.sh" ]; then
    echo "Configuring startup environment in ${deploy_root}/config/local_env.sh"
    cat << EOF > "${deploy_root}/config/local_env.sh"
: \${GALAXY_ROOT_DIR:="$(pwd)"}
: \${GALAXY_JOB_HANDLER_COUNT:=1}
: \${GALAXY_UMASK:=\$(umask)}
: \${GALAXY_LOG_DIR:="${deploy_root}/log"}
: \${GALAXY_BIND_IP:=0.0.0.0}
: \${GALAXY_PORT:=8080}
export GALAXY_ROOT_DIR GALAXY_JOB_HANDLER_COUNT GALAXY_UMASK GALAXY_LOG_DIR GALAXY_BIND_IP GALAXY_PORT
EOF
fi

if [ ! -f "${deploy_root}/bin/galaxyctl" ]; then
    cp scripts/galaxyctl "${deploy_root}/bin/galaxyctl"
    chmod +x "${deploy_root}/bin/galaxyctl"
fi

# TODO: add to requirements?
if [ ! -f "${deploy_root}/data/venv/bin/supervisord" ]; then
    echo "Installing supervisor in Galaxay virtualenv"
    "${deploy_root}/data/venv/bin/pip" install supervisor
fi

