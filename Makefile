.PHONY: all config clear run run_stdin create_test send
include .config/config_env.sh

all: config

config: clear
	@${BUILD_ENV}
	
clear:
	clear 

run: all clear
	@${RUN_DEFAULT}

run_tests: all clear
	@${RUN_TESTS}


