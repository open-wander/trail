package parser

// DetectFormat examines sample log lines and returns the most likely format.
// Traefik CLF and Traefik JSON count together against Combined; within the
// Traefik family, JSON wins only with a strict majority over CLF. Traefik wins
// ties since it's the more specific format.
func DetectFormat(lines []string) Format {
	if len(lines) == 0 {
		return FormatTraefik // default
	}

	traefikHits := 0
	jsonHits := 0
	combinedHits := 0

	for _, line := range lines {
		if line == "" {
			continue
		}
		if isJSONLine(line) {
			if _, err := ParseTraefikJSON(line); err == nil {
				jsonHits++
			}
		} else if traefikRegex.MatchString(line) {
			traefikHits++
		} else if combinedRegex.MatchString(line) {
			combinedHits++
		}
	}

	// Traefik wins ties (it's more specific, and is the default)
	if combinedHits > traefikHits+jsonHits {
		return FormatCombined
	}
	if jsonHits > traefikHits {
		return FormatTraefikJSON
	}
	return FormatTraefik
}
