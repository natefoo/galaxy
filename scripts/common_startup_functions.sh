#!/bin/sh

__CONDA_INFO=

parse_common_args() {
    INITIALIZE_TOOL_DEPENDENCIES=1
    # Pop args meant for common_startup.sh
    add_pid_arg=0
    add_log_arg=0
    while :
    do
        case "$1" in
            --skip-eggs|--skip-wheels|--skip-samples|--dev-wheels|--no-create-venv|--no-replace-pip|--replace-pip|--skip-client-build)
                common_startup_args="$common_startup_args $1"
                shift
                ;;
            --skip-tool-dependency-initialization)
                INITIALIZE_TOOL_DEPENDENCIES=0
                shift
                ;;
            --skip-venv)
                skip_venv=1
                common_startup_args="$common_startup_args $1"
                shift
                ;;
            --stop-daemon|stop)
                common_startup_args="$common_startup_args --stop-daemon"
                circusctl_args="$circusctl_args quit"
                add_pid_arg=1
                stop_daemon_arg_set=1
                shift
                ;;
            --restart|restart)
                circusctl_args="$circusctl_args restart"
                add_pid_arg=1
                add_log_arg=1
                restart_arg_set=1
                daemon_or_restart_arg_set=1
                shift
                ;;
            --daemon|start)
                circusd_args="$circusd_args --daemon --log-output $LOG_FILE"
                gunicorn_args="$gunicorn_args --daemon"
                GALAXY_DAEMON_LOG="$GALAXY_LOG"
                add_pid_arg=1
                add_log_arg=1
                # --daemonize2 waits until after the application has loaded
                # to daemonize, thus it stops if any errors are found
                daemon_or_restart_arg_set=1
                shift
                ;;
            --status|status)
                circusctl_args="$circusctl_args $1"
                add_pid_arg=1
                shift
                ;;
            --wait)
                wait_arg_set=1
                shift
                ;;
            "")
                break
                ;;
            *)
                circusctl_args="$circusctl_args $1"
                shift
                ;;
        esac
    done
}

run_common_start_up() {
    ./scripts/common_startup.sh $common_startup_args || exit 1
}

conda_activate() {
    : ${GALAXY_CONDA_ENV:="_galaxy_"}
    echo "Activating Conda environment: $GALAXY_CONDA_ENV"
    # Dash is actually supported by 4.4, but not with `. /path/to/activate`, only `conda activate`, which we
    # can't load unless we know the path to <conda_root>/etc/profile.d/conda.sh
    if ! command -v source >/dev/null; then
        echo "WARNING: Your shell is not supported with Conda, attempting to use Conda env"
        echo "         '$GALAXY_CONDA_ENV' with manual environment setup. To avoid this"
        echo "         message, use a supported shell or activate the environment before"
        echo "         starting Galaxy."
        PATH="$(get_conda_env_path $GALAXY_CONDA_ENV)/bin:$PATH"
        CONDA_DEFAULT_ENV="$GALAXY_CONDA_ENV"
        CONDA_PREFIX="$(get_conda_active_prefix)"
    else
        source "$(get_conda_root_prefix)"/bin/activate "$GALAXY_CONDA_ENV"
    fi
}

setup_python() {
    # If there is a .venv/ directory, assume it contains a virtualenv that we
    # should run this instance in.
    : ${GALAXY_VIRTUAL_ENV:=.venv}
    # $GALAXY_CONDA_ENV isn't set here to avoid running the version check if not using Conda
    if [ -d "$GALAXY_VIRTUAL_ENV" ] && [ -z "$skip_venv" ]; then
        [ -n "$PYTHONPATH" ] && { echo 'Unsetting $PYTHONPATH'; unset PYTHONPATH; }
        echo "Activating virtualenv at $GALAXY_VIRTUAL_ENV"
        . "$GALAXY_VIRTUAL_ENV/bin/activate"
    elif [ -z "$skip_venv" ]; then
        set_conda_exe
        if [ -n "$CONDA_EXE" ] && \
                check_conda_env ${GALAXY_CONDA_ENV:="_galaxy_"}; then
            # You almost surely have the required minimum pip version and running `conda install ... pip>=<min_ver>` every time is slow
            REPLACE_PIP=0
            [ -n "$PYTHONPATH" ] && { echo 'Unsetting $PYTHONPATH'; unset PYTHONPATH; }
            if [ "$CONDA_DEFAULT_ENV" != "$GALAXY_CONDA_ENV" ]; then
                conda_activate
            fi
            if [ "$CONDA_DEFAULT_ENV" = "base" ] || [ "$CONDA_DEFAULT_ENV" = "root" ]; then
                echo "ERROR: Conda is in 'base' environment, refusing to continue"
                exit 1
            fi
        fi
    fi

    # If you are using --skip-venv we assume you know what you are doing but warn
    # in case you don't.
    [ -n "$PYTHONPATH" ] && echo 'WARNING: $PYTHONPATH is set, this can cause problems importing Galaxy dependencies'

    python ./scripts/check_python.py || exit 1
}

set_galaxy_config_file_var() {
    if [ -z "$GALAXY_CONFIG_FILE" ]; then
        GALAXY_CONFIG_FILE=$(PYTHONPATH=lib python -c "from __future__ import print_function; from galaxy.util.properties import find_config_file; print(find_config_file(['galaxy', 'universe_wsgi']) or '')")
        export GALAXY_CONFIG_FILE
    fi
}

find_server() {
    server_config=$1
    server_app=$2
    arg_getter_args=
    default_webserver="circusd"
    APP_WEBSERVER=${APP_WEBSERVER:-$default_webserver}
    # TODO: use circusd.ini if it exists, else use circusd.ini.sample ?
    CIRCUS_CONFIG_FILE=${CIRCUS_CONFIG_FILE:-config/${server_app}_circus.ini}
    if [ "$APP_WEBSERVER" = "circusd" ]; then
        if [ -n "$circusctl_args" ]; then
            run_server="circusctl"
            server_args="$circusctl_args"
        else
            run_server="circusd"
            export GALAXY_DAEMON_LOG=$GALAXY_DAEMON_LOG
            server_args="$CIRCUS_CONFIG_FILE $circusd_args"
        fi
    else
        echo "WEBSERVER method $APP_WEBSERVER not supported"
    fi
}

# Prior to Conda 4.4, the setup method was to add <conda_root>/bin to $PATH. Beginning with 4.4, that method is still
# possible, but the preferred method is to source <conda_root>/etc/profile.d/conda.sh. If the new method is used, the
# base environment will *not* be on $PATH, unlike previous versions, and `conda` is a shell function not available to
# subshells (e.g. scripts). Additionally, in Conda >= 4.4 (and sometimes in Conda < 4.4 due to bugs), an activated
# non-root/base environment will not have a symlink to `conda`. Beginning with 4.5, $CONDA_EXE will be set to the path
# to the `conda` script in the base environment. Thus in Conda 4.4, it may not be possible to locate `conda` even if you
# are using Conda.
set_conda_exe() {
    [ -n "$CONDA_EXE" ] || [ -n "$_CONDA_EXE_SET" ] && return 0
    if python -V 2>&1 | grep -q -e 'Anaconda' -e 'Continuum Analytics' || \
            python -c 'import sys; print(sys.version.replace("\n", " "))' 2>/dev/null | grep -q -e 'packaged by conda-forge' ; then
        CONDA_EXE=$(command -v conda)
        if [ -z "$CONDA_EXE" ]; then
            echo "WARNING: \`python\` is from conda, but the \`conda\` command cannot be found."
            pydir="$(dirname "$(command -v python)")"
            for CONDA_EXE in $pydir/conda $pydir/../../../bin/conda; do
                [ -x "$CONDA_EXE" ] && break || unset CONDA_EXE
            done
            if [ -z "$CONDA_EXE" ]; then
                echo "WARNING: Unable to guess conda location, if you are using Conda 4.4, upgrade to"
                echo "         Conda 4.5 or activate the base environment prior to starting Galaxy:"
                echo "         $ conda activate base"
            else
                echo "Guessed conda location: $CONDA_EXE"
                PATH="$(dirname "$CONDA_EXE"):$PATH"
            fi
        else
            echo "Found conda at: $CONDA_EXE"
        fi
        _CONDA_EXE_SET=1
    fi
}

set_conda_info() {
    # cache conda info to avoid the cost of running it multiple times
    if [ -z "$__CONDA_INFO" ]; then
        __CONDA_INFO="$(${CONDA_EXE:-conda} info --json)"
    fi
}

get_conda_active_prefix() {
    set_conda_info
    printf "%s" "$__CONDA_INFO" \
        | python -c "import json, sys; print(json.load(sys.stdin)['active_prefix'])"
}

get_conda_root_prefix() {
    set_conda_info
    printf "%s" "$__CONDA_INFO" \
        | python -c "import json, sys; print(json.load(sys.stdin)['root_prefix'])"
}

check_conda_env() {
    # envs listed in ~/.conda/environments.txt show up in envs.txt but can't be activated by name. =/
    set_conda_info
    printf "%s" "$__CONDA_INFO" \
        | python -c "import json, os.path, sys; info = json.load(sys.stdin); sys.exit(0 if '$1' in [os.path.basename(p) for p in info['envs'] if os.path.dirname(p) in info['envs_dirs']] else 1)"
}

get_conda_env_path() {
    set_conda_info
    printf "%s" "$__CONDA_INFO" \
        | python -c "import json, os.path, sys; info = json.load(sys.stdin); print([p for p in info['envs'] if os.path.basename(p) == '$1' and os.path.dirname(p) in info['envs_dirs']][0])"
}
