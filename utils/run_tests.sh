#! /bin//bash

# Get configuration constants
if [ -z "${CONFIG_PATH}" ]; then 
	CONFIG_PATH=.config/config_env.sh
fi

if [ -z "${__CONFIG__}" ]; then 
	source "${CONFIG_PATH}"
	if [ -z "${__CONFIG__}" ]; then 
		return 1
	fi
fi

# Activate venv
source ${UTILS}/activate_venv.sh


RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
RESET='\033[0m'  # Сброс цвета

DATABASE=tests/input.csv
OUTDIR=output
CONFIG=tests/config.xml
MY_BRF1=output/BRF1.csv
ANSWER_BRF1=answers/BRF1_correct.csv

COUNT_TESTS=5

# Run program

echo -e "${YELLOW}Run tests ${RESET}"

for ((i = 0; i < ${COUNT_TESTS}; ++i))
do
    echo -e "${CYAN}TEST $((i + 1))${RESET}"
    python main.py ${DATABASE} --outdir ${OUTDIR} --config ${CONFIG} --clean-output-dir --sort-by-moment &> /dev/null
    
    differs=$(diff -w output/BRF1.csv answers/BRF1_correct.csv)
    if [ "${differs}" = "" ]
    then 
        echo -e "${YELLOW}Result: ${GREEN}Correct ${RESET}"
    else
        echo -e "${YELLOW}Result: ${RED}Incorrect ${RESET}"
        echo -e "${RED}Differs in path log/differs ${RESET}"
        echo ${differs} > logs/differs
        break
    fi
done 
echo -e "${YELLOW}All tests complited ${RESET}"