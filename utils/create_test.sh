#!/usr/bin/env bash
# create_test.sh
#
# Читает из stdin:
#   1. первую строку до \n  → имя файла (если непустое)
#   2. всё остальное до EOF → содержимое
# Если имя пустое — генерирует следующий числовой префикс.
#
# Результат: tests/{name}_test

set -euo pipefail

# 1) Папка для тестов
dir="tests"
ansdir="answers"

mkdir -p "$dir" "${ansdir}"

# 2) Чтобы паттерн не зависал, если файлов нет
shopt -s nullglob

name=""


if [[ -z "$name" ]]; then
  declare -A used  # Ассоциативный массив

  for f in "$dir"/*_test; do
    base=${f##*/}            # Получаем имя файла без пути
    num=${base%%_*}          # Извлекаем номер до _
    if [[ $num =~ ^[0-9]+$ ]]; then
      used["$num"]=1         # Отмечаем, что номер занят
    fi
  done

  i=1
  while true; do
    if [[ -z "${used[$i]+unset}" ]]; then
      name=$i
      break
    fi
    ((i++))
  done
fi

# 6) Собираем имя выходного файла
outfile="$dir/${name}_test"

# 7) Печатаем приглашение для ввода содержимого файла
echo "Введите содержимое для файла '$outfile'. Завершите ввод, нажав Ctrl+D."

# 8) Читаем **всё**, что осталось в stdin (после первой строки), в файл
cat > "$outfile"

# 9) Сообщаем пользователю о результате
echo "Файл '$outfile' был успешно создан!"

ansfile="$ansdir/${name}_test"
echo "Введите содержимое для файла '$ansfile'. Завершите ввод, нажав Ctrl+D."

cat > "$ansfile"

echo "Файл '$ansfile' был успешно создан!"

