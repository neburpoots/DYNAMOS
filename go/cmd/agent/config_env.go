package main

import (
	"os"
	"strconv"
	"strings"
)

func queueDeleteAfterFromEnv() int64 {
	const fallbackSeconds int64 = 600

	rawValue := strings.TrimSpace(os.Getenv("DYNAMOS_QUEUE_DELETE_AFTER_SECONDS"))
	if rawValue == "" {
		return fallbackSeconds
	}

	seconds, err := strconv.ParseInt(rawValue, 10, 64)
	if err != nil || seconds <= 0 {
		return fallbackSeconds
	}

	return seconds
}
