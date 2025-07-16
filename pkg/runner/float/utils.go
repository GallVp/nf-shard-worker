package float

import (
	"fmt"
	"regexp"
)

func extractMountPaths(input string) []string {
	re := regexp.MustCompile(`(--dataVolume)\s+(\[(?:[^\]]*)\]s3://[^:\s]+:[^\s']+)`)
	matches := re.FindAllStringSubmatch(input, -1)
	var result []string
	for _, match := range matches {
		if len(match) >= 3 {
			result = append(result, match[1], match[2])
		}
	}
	return result
}

func validateBucketName(name string) error {

	bucketPattern := `^https:\/\/\S+\.s3\.[a-z]{2}-[a-z]+-[0-9]\.amazonaws\.com\/?$`
	re := regexp.MustCompile(bucketPattern)
	if re.MatchString(name) {
		return nil
	}

	return fmt.Errorf("bucket name: %s does not conform to pattern: %s", name, bucketPattern)
}
