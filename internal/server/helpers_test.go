package server

import "testing"

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		name     string
		bytes    int64
		expected string
	}{
		{"zero bytes", 0, "0 B"},
		{"bytes", 512, "512 B"},
		{"kilobytes", 1536, "1.5 KB"},
		{"megabytes", 1048576, "1.0 MB"},
		{"gigabytes", 1073741824, "1.0 GB"},
		{"large value", 5368709120, "5.0 GB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatBytes(tt.bytes)
			if result != tt.expected {
				t.Errorf("formatBytes(%d) = %s, want %s", tt.bytes, result, tt.expected)
			}
		})
	}
}

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		name     string
		number   int64
		expected string
	}{
		{"zero", 0, "0"},
		{"small", 42, "42"},
		{"hundreds", 999, "999"},
		{"thousands", 1234, "1,234"},
		{"millions", 1234567, "1,234,567"},
		{"large", 1234567890, "1,234,567,890"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := formatNumber(tt.number)
			if result != tt.expected {
				t.Errorf("formatNumber(%d) = %s, want %s", tt.number, result, tt.expected)
			}
		})
	}
}

func TestPct(t *testing.T) {
	tests := []struct {
		name     string
		value    int64
		max      int64
		expected int
	}{
		{"zero value", 0, 100, 0},
		{"zero max", 50, 0, 0},
		{"50 percent", 50, 100, 50},
		{"100 percent", 100, 100, 100},
		{"minimum visibility", 1, 1000, 1},
		{"round down", 49, 100, 49},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pct(tt.value, tt.max)
			if result != tt.expected {
				t.Errorf("pct(%d, %d) = %d, want %d", tt.value, tt.max, result, tt.expected)
			}
		})
	}
}

func TestStatusColor(t *testing.T) {
	tests := []struct {
		name     string
		class    string
		expected string
	}{
		{"2xx success", "2xx", "var(--success)"},
		{"3xx redirect", "3xx", "var(--brand)"},
		{"4xx client error", "4xx", "var(--warning)"},
		{"5xx server error", "5xx", "var(--error)"},
		{"unknown", "unknown", "var(--text-secondary)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := statusColor(tt.class)
			if result != tt.expected {
				t.Errorf("statusColor(%s) = %s, want %s", tt.class, result, tt.expected)
			}
		})
	}
}

func TestGenerateRedirectSuggestion(t *testing.T) {
	tests := []struct {
		path        string
		wantApache  string
		wantTraefik string
	}{
		{"/wp-login.php", "Redirect 301 /wp-login.php /", "middlewares:\n  redirect_wp-login_php:\n    redirectRegex:\n      regex: \"^/wp-login.php$\"\n      replacement: \"/\"\n      permanent: true"},
		{"/wp-admin/setup-config.php", "Redirect 301 /wp-admin/setup-config.php /", "middlewares:\n  redirect_wp-admin_setup-config_php:\n    redirectRegex:\n      regex: \"^/wp-admin/setup-config.php$\"\n      replacement: \"/\"\n      permanent: true"},
		{"/.env", "Redirect 301 /.env /", "middlewares:\n  redirect__env:\n    redirectRegex:\n      regex: \"^/.env$\"\n      replacement: \"/\"\n      permanent: true"},
		{"/foo", "Redirect 301 /foo /", "middlewares:\n  redirect_foo:\n    redirectRegex:\n      regex: \"^/foo$\"\n      replacement: \"/\"\n      permanent: true"},
		{"/XMLRPC.php", "Redirect 301 /XMLRPC.php /", "middlewares:\n  redirect_xmlrpc_php:\n    redirectRegex:\n      regex: \"^/XMLRPC.php$\"\n      replacement: \"/\"\n      permanent: true"},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := generateRedirectSuggestion(tt.path); got != tt.wantApache {
				t.Errorf("generateRedirectSuggestion(%q) = %q, want %q", tt.path, got, tt.wantApache)
			}
			if got := generateTraefikSnippet(tt.path); got != tt.wantTraefik {
				t.Errorf("generateTraefikSnippet(%q) = %q, want %q", tt.path, got, tt.wantTraefik)
			}
		})
	}
}

func TestApplyRedirectSuggestions(t *testing.T) {
	paths := []PathStat{{Path: "/wp-login.php"}, {Path: "/foo"}}
	applyRedirectSuggestions(paths)
	if paths[0].Suggestion == "" {
		t.Errorf("expected suggestion for %s", paths[0].Path)
	}
	if paths[0].TraefikSuggestion == "" {
		t.Errorf("expected Traefik suggestion for %s", paths[0].Path)
	}
	if paths[1].Suggestion == "" {
		t.Errorf("expected suggestion for %s", paths[1].Path)
	}
	if paths[1].TraefikSuggestion == "" {
		t.Errorf("expected Traefik suggestion for %s", paths[1].Path)
	}
}
