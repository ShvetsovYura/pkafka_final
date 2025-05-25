#!/bin/bash

# Переходим в директорию проекта
cd "$(dirname "$0")" || exit

# Массив с именами поддиректорий в cmd
APPS=("app1" "app2" "app3")

# Функция для запуска приложения
start_app() {
  echo "Запуск приложения $1..."
  cd "cmd/$1" || return
  go build -o "../../bin/$1" || return
  "../../bin/$1" &
  cd ../..
}

# Создаем директорию bin, если её нет
mkdir -p bin

# Запускаем все приложения
for app in "${APPS[@]}"; do
  start_app "$app"
done

# Ждем завершения всех процессов
wait