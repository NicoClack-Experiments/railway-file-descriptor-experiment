package main

import (
	"crypto/rand"
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
	filePath := os.Getenv("FILE_PATH")
	if filePath == "" {
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

	reuseHandle := os.Getenv("REUSE_HANDLE") == "true"
	keepOpen := os.Getenv("KEEP_OPEN") == "true"
	totalWritten := 0
	lastLoggedAt := 0
	openHandles := 0
	if reuseHandle {
		openHandles = 1
	}
	data := make([]byte, chunkSize) // Reuse buffer for efficiency

	fmt.Printf("Starting writer: FILE_PATH=%s, DELAY=%dms, REUSE_HANDLE=%v, KEEP_OPEN=%v\n", filePath, delayMs, reuseHandle, keepOpen)

	var f *os.File
	if reuseHandle {
		var err error
		f, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("Error opening file: %v", err)
		}
		defer f.Close()
	}

	for {
		// Fill buffer with random data each time
		if _, err := rand.Read(data); err != nil {
			log.Printf("Error generating random data: %v", err)
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
			continue
		}

		var currF *os.File
		if reuseHandle {
			currF = f
			if _, err := currF.Seek(0, 0); err != nil {
				log.Printf("Error seeking file: %v", err)
				time.Sleep(time.Duration(delayMs) * time.Millisecond)
				continue
			}
		} else {
			var err error
			// Open file with O_TRUNC to overwrite each time
			currF, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
			if err != nil {
				log.Printf("Error opening file: %v", err)
				time.Sleep(time.Duration(delayMs) * time.Millisecond)
				continue
			}
			if keepOpen {
				openHandles++
			}
		}

		n, err := currF.Write(data)
		if err != nil {
			log.Printf("Error writing to file: %v", err)
			if !reuseHandle && !keepOpen {
				currF.Close()
			}
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
			continue
		}

		if !reuseHandle && !keepOpen {
			currF.Close()
		}

		totalWritten += n
		if totalWritten-lastLoggedAt >= logEvery {
			fmt.Printf("Total written: %d MB, Open handles: %d\n", totalWritten/(1024*1024), openHandles)
			lastLoggedAt = totalWritten
		}

		time.Sleep(time.Duration(delayMs) * time.Millisecond)
	}
}
