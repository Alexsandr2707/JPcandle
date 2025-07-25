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

if [ ! -e "${CACHE}" ]; then 
	mkdir -p "${CACHE}"
fi

if [ ! -e "${VENV}" ]; then 
	${VENV_BUILDER} ${VENV}

	# Specify path to Interpriter for vscode
	(echo ""; echo "PYTHONPATH=${INTERP}") >> "${VSCODE_ENV}"
	
	if [ ! -e "${REQUIREMENTS}" ]; then 
		touch "${REQUIREMENTS}"
	fi

fi

if [ -e "${INSTALLED_REQUIREMENTS}" ]; then 
	 if ! cmp -s "${REQUIREMENTS}" "${INSTALLED_REQUIREMENTS}"; then
		source ${VENV}/bin/activate
		pip install -r ${REQUIREMENTS}
		deactivate

		cp "${REQUIREMENTS}" "${INSTALLED_REQUIREMENTS}"
	fi
	
else	
	if [ -s "${REQUIREMENTS}" ]; then 
		source ${VENV}/bin/activate
		pip install -r ${REQUIREMENTS}
		deactivate
	fi

	cp "${REQUIREMENTS}" "${INSTALLED_REQUIREMENTS}"
fi
