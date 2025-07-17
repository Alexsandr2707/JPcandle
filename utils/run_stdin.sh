#! /bin/bash

if [ -z "${CONFIG_PATH}" ]; then
        CONFIG_PATH=.config/config_env.sh
fi

if [ -z "${__CONFIG__}" ]; then
        source "${CONFIG_PATH}"
        if [ -z "${__CONFIG__}" ]; then
                return 1
        fi
fi

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
RESET='\033[0m'  # Сброс цвета


ARGS=(${DEF_ARGS[@]} ${@:1})
CMD="${INTERP} ${PYFILE} ${ARGS[@]}"

echo -e "${CYAN}Run ${YELLOW}${CMD}${CYAN} in ${YELLOW}stdin${RESET} mode"

source ${VENV}/bin/activate
${CMD}
deactivate

