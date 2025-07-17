#!/bin/bash


if [ -z "${CONFIG_PATH}" ]; then
        CONFIG_PATH=.config/config_env.sh
fi

if [ -z "${__CONFIG__}" ]; then
        source "${CONFIG_PATH}"
        if [ -z "${__CONFIG__}" ]; then
                return 1
        fi
fi

source ${VENV}/bin/activate
