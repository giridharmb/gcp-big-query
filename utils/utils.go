package utils

import (
	"fmt"
	"os"
	"regexp"
)

// IsValidString
// checks if the input string contains only alpha-numeric characters,
// hyphens, underscores.
func IsValidString(s string) bool {
	// Compile the regular expression that matches allowed characters.
	// ^ indicates the start of the string.
	// [a-zA-Z0-9_-] specifies allowed characters (alpha-numeric, underscore, and hyphen).
	// + indicates one or more of the allowed characters.
	// $ indicates the end of the string.
	// The entire expression means: from start to end, all characters must be within the allowed set.
	re := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
	return re.MatchString(s)
}

func DoStuff() {
	tests := []string{
		"valid-string_123",
		"also_valid-string.123",
		"invalid|string",
		"also&invalid",
	}

	for _, test := range tests {
		fmt.Printf("'%s' is valid: %t\n", test, IsValidString(test))
	}
}

// SplitIntoChunks splits a slice of any type into chunks of a specified size.
func SplitIntoChunks[T any](slice []T, chunkSize int) [][]T {
	var chunks [][]T
	for {
		if len(slice) == 0 {
			break
		}

		// Calculate the end index for the current chunk
		end := chunkSize
		if end > len(slice) {
			end = len(slice)
		}

		// Append the current chunk to the chunks slice
		chunks = append(chunks, slice[:end])

		// Move the start index for the next chunk
		slice = slice[end:]
	}
	return chunks
}

func TryChunkSplit() {
	// Example: splitting a slice of integers
	nums := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	chunks := SplitIntoChunks(nums, 3)

	for i, chunk := range chunks {
		fmt.Printf("Chunk %d: %v\n", i+1, chunk)
	}

	// Example: splitting a slice of strings
	strings := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	stringChunks := SplitIntoChunks(strings, 4)

	for i, chunk := range stringChunks {
		fmt.Printf("String Chunk %d: %v\n", i+1, chunk)
	}
}

func FileExists(fileName string) bool {
	info, err := os.Stat(fileName)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
