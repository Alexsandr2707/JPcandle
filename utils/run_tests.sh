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

if [ "$(ls tests)" == "" ]; then 
	echo -e "${RED}Files not found${RESET}"
	${RUN_STDIN}
	exit 0
fi

CMD="${INTERP} ${PYFILE}"
OUTPUTS=outputs
ANSWERS=answers

mkdir -p ${OUTPUTS}

get_infiles() {
	# Исходная переменная files — возможно, уже с чем-то
	local files=( $(ls tests/*_test 2>/dev/null | sort -V) )

	# Получим все *_test файлы в той же папке
	local all_files=( $(ls tests/* 2>/dev/null) )

	# Добавим отсутствующие файлы в массив files
	for f in "${all_files[@]}"; do
  		skip=false
  		for existing in "${files[@]}"; do
	    	if [[ "$existing" == "$f" ]]; then
    			skip=true
      			break
    		fi
  		done
  	if ! $skip; then
    	files+=("$f")
  	fi
	done

	# Вывод результата (отсортированный)
	printf "%s\n" "${files[@]}" 
}

print_divider() {
  local color="\033[1;34m"  # синий жирный
  local reset="\033[0m"
  printf "${color}%*s${reset}\n" "$(tput cols)" '' | tr ' ' '='
}

print_section() {
  local title="$1"
  local result=" $2"

  local title_color="\033[1;36m" # бирюзовый
  local	result_color="$3"
  local reset="\033[0m"

  local width=$(tput cols)
  local filler="─"
  local padding=$(( (width - ${#title} - ${#result} - 2) / 2 ))
  printf "${title_color}%*s %s${result_color}%s %*s${reset}\n" $padding '' "$title" "$result" $padding ''
}

is_true_answer() { # 1-outfile 2-ansfile
	local outfile="$1"
	local ansfile="$2"
	if [ -f "$ansfile" ] && grep -q '[^[:space:]]' "$ansfile"; then
		if diff -u "$ansfile" "$outfile" > /tmp/file_diff_output; then
			echo -n "true"
		else 
			echo -n "false"
		fi
	else 
		echo -n "unknown"
	fi
}

print_result() { # 1-infile 2-outfile 3-errfile 4-ansfile 5-result 6-flag
	local infile="$1"
	local outfile="$2"
	local errfile="$3"
	local ansfile="$4"
	local result="$5"
	local flag="$6"
	local result_msg
	local result_color

	if [ "$result" == "true" ]; then 
		result_msg="Correct"
		result_color="${GREEN}"
	elif [ "$result" == "false" ]; then
		result_msg="BAD ANSWER!"
		result_color="${RED}"
	else 
		result_msg="Answer is unknown."
		result_color="${YELLOW}"
	fi

	print_divider
	print_section "TEST ${infile}:" "${result_msg}" "${result_color}"
	print_divider
	
	if [ "$result" == "true" ]; then 
		return 0
	fi

	print_section "Input"
	echo -e "${GREEN}$(head -n 10 "$infile")${RESET}"

	print_section "Output"
	cat ${outfile}
	
	if [ -f "$ansfile" ] && grep -q '[^[:space:]]' "$ansfile"; then
		if diff -u "$ansfile" "$outfile" > /tmp/file_diff_output; then
  			echo -e "${GREEN}Correct output.${RESET}"
		else
			print_section "Answer"
			echo -e "${GREEN}$(cat "$ansfile")${RESET}"
	
			print_section "Check Answer"
		
  			echo -e "${RED}BAD ANSWER! Differences:${RESET}"
  			echo
  			# Раскрашиваем diff-подсветку
  			while IFS= read -r line; do
    			case "$line" in
      			---*) echo -e "${YELLOW}$line${RESET}" ;;
      			+++*) echo -e "${YELLOW}$line${RESET}" ;;
     	 		+*)   echo -e "${GREEN}$line${RESET}" ;;
      			-*)   echo -e "${RED}$line${RESET}" ;;
      			*)    echo "$line" ;;
    			esac
  			done < /tmp/file_diff_output
		fi

	fi

	if [ "$(cat "${errfile}")" != "" ]; then 
		echo -e "${CYAN}Errors:${RESET}"
		echo -e "${RED}$(cat "${errfile}")${RESET}"
	fi
}

echo -e "${CYAN}TESTING ${YELLOW}${CMD}${RESET}"

for infile in $(get_infiles); do

	outfile=${OUTPUTS}/"${infile/'tests/'}"_stdout
	errfile=${OUTPUTS}/"${infile/'tests/'}"_stderr
	ansfile=${ANSWERS}/"${infile/'tests/'}"
	
	source ${VENV}/bin/activate
	RUN="$CMD --database-csv $infile"
	${RUN} 1> ${outfile} 2> ${errfile}
	deactivate
	
	result="$(is_true_answer "$outfile" "$ansfile")"
	print_result "$infile" "$outfile" "$errfile" "$ansfile" "$result"
	

done



