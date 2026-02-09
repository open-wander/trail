package parser

// DetectFormat examines sample log lines and returns the most likely format.
// Traefik wins ties since it's the more specific format.
func DetectFormat(lines []string) Format {
	if len(lines) == 0 {
		return FormatTraefik // default
	}

	traefikHits := 0
	combinedHits := 0

	for _, line := range lines {
		if line == "" {
			continue
		}
		if traefikRegex.MatchString(line) {
			traefikHits++
		} else if combinedRegex.MatchString(line) {
			combinedHits++
		}
	}

	// Traefik wins ties (it's more specific, and is the default)
	if combinedHits > traefikHits {
		return FormatCombined
	}
	return FormatTraefik
}
