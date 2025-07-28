__CONFIG__=1

SRC=.
PYFILE=${SRC}/main.py

CONFIGS=.config
CONFIG_ENV=${CONFIGS}/config_env.sh

VENV_BUILDER=p"ython3.12 -m venv"
VENV=venv
INTERP=${VENV}/bin/python
REQUIREMENTS=${CONFIGS}/requirements.txt

VSCODE_ENV=.env

UTILS=utils
BUILD_ENV=${UTILS}/build_env.sh
ACTIVATE_VENV=${UTILS}/activate_venv.sh
RUN_TESTS=${UTILS}/run_tests.sh
RUN_DEFAULT=${UTILS}/run_default.sh

CACHE=.cache
INSTALLED_REQUIREMENTS=${CACHE}/requirements.txt.installed


CONFIG_XML=${CONFIGS}/config.xml
DATABASE_CSV=${SRC}/database-csv/input.csv
OUTDIR=${SRC}/output
CLEAN="--clean-output-dir"
SORT="--sort-by-moment"

DEF_ARGS=(${DATABASE_CSV} --config ${CONFIG_XML} --outdir ${OUTDIR} ${CLEAN} ${SORT})
