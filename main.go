package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

const (
	chunkSize = 10 * 1024 * 1024 // 10MB
	logEvery  = 100 * 1024 * 1024 // 100MB
)

func main() {
	mountPath := os.Getenv("FILE_PATH")
	if mountPath == "" {
		log.Fatal("FILE_PATH environment variable is required")
	}

	delayStr := os.Getenv("DELAY")
	if delayStr == "" {
		log.Fatal("DELAY environment variable is required")
	}

	delayMs, err := strconv.Atoi(delayStr)
	if err != nil {
		log.Fatalf("Invalid DELAY value: %v", err)
	}

	totalWritten := 0
	lastLoggedAt := 0
	data := make([]byte, chunkSize) // Reuse buffer for efficiency

	fmt.Printf("Starting writer: FILE_PATH=%s, DELAY=%dms\n", mountPath, delayMs)

	for {
		// Open file with O_TRUNC to overwrite each time
		f, err := os.OpenFile(mountPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Printf("Error opening file: %v", err)
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
			continue
		}

		n, err := f.Write(data)
		if err != nil {
			log.Printf("Error writing to file: %v", err)
			f.Close()
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
			continue
		}

		f.Close()

		totalWritten += n
		if totalWritten-lastLoggedAt >= logEvery {
			fmt.Printf("Total written: %d MB\n", totalWritten/(1024*1024))
			lastLoggedAt = totalWritten
		}

		time.Sleep(time.Duration(delayMs) * time.Millisecond)
	}
}
