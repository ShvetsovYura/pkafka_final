package main

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	apps := []string{"blocker", "client_api", "shop_api", "analytics"} // Список приложений в cmd/

	for _, app := range apps {
		cmdPath := filepath.Join("cmd", app, "main.go")

		// Команда для запуска: go run cmd/[app]/main.go
		cmd := exec.Command("go", "run", cmdPath)

		// Перенаправляем вывод в консоль
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		// Запускаем в фоновом режиме
		if err := cmd.Start(); err != nil {
			log.Printf("Ошибка запуска %s: %v\n", app, err)
		} else {
			fmt.Printf("Приложение %s запущено (PID: %d)\n", app, cmd.Process.Pid)
		}
	}

	fmt.Println("Все приложения запущены. Нажмите Ctrl+C для остановки.")
	select {} // Бесконечное ожидание (можно заменить на WaitGroup)
}
