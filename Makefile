.PHONY: all config clear run run_stdin create_test send
include .config/config_env.sh

all: config

config: clear
	@${BUILD_ENV}
	
clear:
	clear 

run: all clear
	@${RUN_DEFAULT}

run_tests:
	@${RUN_TESTS}

run_stdin: all clear
	@${RUN_STDIN}

create_test: clear
	@${CREATE_TEST}


