package server

import (
	"bytes"
	"fmt"
	"log"
	"time"

	"github.com/gofiber/fiber/v2"
)

// DonutSegment represents one segment of a donut chart
type DonutSegment struct {
	Label string
	Count int64
	Pct   float64
	Color string
	Start float64
	End   float64
}

// computeDonutPositions fills in Pct, Start, End for donut segments
func computeDonutPositions(segments []DonutSegment) {
	var total int64
	for _, s := range segments {
		total += s.Count
	}
	if total == 0 {
		return
	}
	cumPct := 0.0
	for i := range segments {
		segments[i].Pct = float64(segments[i].Count) / float64(total) * 100
		segments[i].Start = cumPct
		cumPct += segments[i].Pct
		segments[i].End = cumPct
	}
	if len(segments) > 0 {
		segments[len(segments)-1].End = 100
	}
}

// OverviewData represents the data for the overview template
type OverviewData struct {
	Stats         *TotalStat
	RequestsChart []TimeSeriesPoint
	VisitorsChart []TimeSeriesPoint
	TopPaths      []PathStat
	StatusCodes   []StatusStat
	TopReferrers  []ReferrerStat
	NotFoundPaths []PathStat
	UserAgents    []UserAgentStat
	Methods       []MethodStat
	StatusDetails []SpecificStatusStat
	HourOfDay     []HourOfDayStat
	MaxRequests   int64
	MaxVisitors   int64
	MaxStatus     int64
	MaxNotFound   int64
	MaxUserAgent  int64
	MaxMethod     int64
	MaxStatusDet  int64
	MaxHourOfDay  int64
	MaxReferrer   int64
	Range         string
	CustomFrom    string
	CustomTo      string
	Router        string
	IncludeBots   bool
	Routers       []string
	Page          string
	// Donut chart data
	StatusDonut    []DonutSegment
	MethodDonut    []DonutSegment
	UserAgentDonut []DonutSegment
	// Hour of day visitors overlay
	HourVisitors    []HourOfDayStat
	MaxHourVisitors int64
	// New analytics panels
	Countries         []CountryStat
	Browsers          []BrowserStat
	OSStats           []OSStat
	BrowserDonut      []DonutSegment
	OSDonut           []DonutSegment
	DurationHist      []DurationBucketStat
	Percentiles       *PercentileResult
	BandwidthChart    []TimeSeriesPoint
	ResponseTimeChart []TimeSeriesPoint
	MobilePct         float64
	DesktopPct        float64
	GeoIPEnabled      bool
	MaxCountry        int64
	MaxBrowser        int64
	MaxOS             int64
	MaxDurationHist   int64
	MaxBandwidth      int64
	MaxResponseTime   int64
	ActiveTab         string
}

// SecurityData represents the data for the security template
type SecurityData struct {
	TotalUnrouted  int64
	BotPct         float64
	HumanPct       float64
	Total5xx       int64
	SlowestAvgMs   int64
	ThreatPatterns []ThreatPatternStat
	BotBreakdown   []UserAgentStat
	HumanCount     int64
	BotCount       int64
	TotalTraffic   int64
	MaxThreat      int64
	ErrorTrends    []TimeSeriesPoint
	MaxErrorCount  int64
	ErrorPaths     []PathStat
	SlowestPaths   []PathStat
	Range          string
	CustomFrom     string
	CustomTo       string
	Page           string
	ActiveTab      string
}

// handleOverview serves the main dashboard overview page
func (s *Server) handleOverview(c *fiber.Ctx) error {
	data, err := s.getOverviewData(c)
	if err != nil {
		log.Printf("Error loading overview data: %v", err)
		return c.Status(500).SendString("Error loading dashboard data")
	}

	var buf bytes.Buffer
	if err := s.overviewTmpl.ExecuteTemplate(&buf, "layout.html", data); err != nil {
		log.Printf("Error rendering template: %v", err)
		return c.Status(500).SendString("Error rendering page")
	}

	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.Send(buf.Bytes())
}

// handleSecurity serves the security dashboard page
func (s *Server) handleSecurity(c *fiber.Ctx) error {
	data, err := s.getSecurityData(c)
	if err != nil {
		log.Printf("Error loading security data: %v", err)
		return c.Status(500).SendString("Error loading security data")
	}

	var buf bytes.Buffer
	if err := s.securityTmpl.ExecuteTemplate(&buf, "layout.html", data); err != nil {
		log.Printf("Error rendering template: %v", err)
		return c.Status(500).SendString("Error rendering page")
	}

	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.Send(buf.Bytes())
}

// validOverviewTabs is the set of valid tab names for the overview page
var validOverviewTabs = map[string]bool{
	"summary":     true,
	"traffic":     true,
	"status":      true,
	"devices":     true,
	"performance": true,
}

// handleAPIOverview serves the htmx partial for overview data
func (s *Server) handleAPIOverview(c *fiber.Ctx) error {
	data, err := s.getOverviewData(c)
	if err != nil {
		log.Printf("Error loading overview data: %v", err)
		return c.Status(500).SendString("Error loading dashboard data")
	}

	tmplName := "overview_tab_" + data.ActiveTab + ".html"

	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, tmplName, data); err != nil {
		log.Printf("Error rendering tab template %s: %v", tmplName, err)
		return c.Status(500).SendString("Error rendering partial")
	}

	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.Send(buf.Bytes())
}

// handleAPISecurity serves the htmx partial for security data
func (s *Server) handleAPISecurity(c *fiber.Ctx) error {
	return s.handleAPISecurityPartial(c)
}

// handleAPIFilters serves the htmx partial for filter options
func (s *Server) handleAPIFilters(c *fiber.Ctx) error {
	return c.SendString("API Filters - not yet implemented")
}

// getOverviewData fetches and prepares data for the overview page
func (s *Server) getOverviewData(c *fiber.Ctx) (*OverviewData, error) {
	// Parse query parameters
	router := c.Query("router", "")
	includeBots := c.Query("bots", "false") == "true"
	activeTab := c.Query("tab", "summary")
	if !validOverviewTabs[activeTab] {
		activeTab = "summary"
	}

	// Calculate time filter based on range (supports custom dates)
	filter, rangeParam := s.buildFilterWithCustom(c, router, includeBots)
	customFrom := c.Query("custom_from", "")
	customTo := c.Query("custom_to", "")
	log.Printf("Overview query: range=%s router=%q bots=%v from=%s to=%s",
		rangeParam, router, includeBots, filter.From, filter.To)

	// Fetch all required data
	stats, err := s.queries.TotalStats(filter)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch total stats: %w", err)
	}
	log.Printf("Overview: total stats loaded (requests=%d visitors=%d)", stats.Requests, stats.Visitors)

	// Use daily rollup for multi-day ranges, hourly for today
	useDaily := rangeParam == "7d" || rangeParam == "30d" || rangeParam == "custom"
	var requestsChart, visitorsChart []TimeSeriesPoint
	if useDaily {
		requestsChart, err = s.queries.DailyRequestsOverTime(filter)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch daily requests: %w", err)
		}
		visitorsChart, err = s.queries.DailyVisitors(filter)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch daily visitors: %w", err)
		}
	} else {
		requestsChart, err = s.queries.RequestsOverTime(filter)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch requests over time: %w", err)
		}
		visitorsChart, err = s.queries.UniqueVisitors(filter)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch visitors over time: %w", err)
		}
	}

	topPaths, err := s.queries.TopPaths(filter, 10)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch top paths: %w", err)
	}

	statusCodes, err := s.queries.StatusBreakdown(filter)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch status breakdown: %w", err)
	}

	referrers, err := s.queries.TopReferrers(filter, 10)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch top referrers: %w", err)
	}

	notFoundPaths, err := s.queries.TopNotFound(filter, 10)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch 404 paths: %w", err)
	}

	userAgents, err := s.queries.UserAgentBreakdown(filter)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch user agents: %w", err)
	}

	methods, err := s.queries.MethodBreakdown(filter)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch method breakdown: %w", err)
	}

	statusDetails, err := s.queries.SpecificStatusCodes(filter)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch specific status codes: %w", err)
	}

	hourOfDay, err := s.queries.HourOfDayDistribution(filter)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch hour of day: %w", err)
	}

	log.Printf("Overview: all queries complete (paths=%d referrers=%d agents=%d methods=%d statuses=%d hours=%d 404s=%d)",
		len(topPaths), len(referrers), len(userAgents), len(methods), len(statusDetails), len(hourOfDay), len(notFoundPaths))

	// Calculate max values for bar chart scaling
	maxRequests := int64(1)
	for _, point := range requestsChart {
		if point.Count > maxRequests {
			maxRequests = point.Count
		}
	}

	maxVisitors := int64(1)
	for _, point := range visitorsChart {
		if point.Count > maxVisitors {
			maxVisitors = point.Count
		}
	}

	maxStatus := int64(1)
	for _, status := range statusCodes {
		if status.Count > maxStatus {
			maxStatus = status.Count
		}
	}

	maxNotFound := int64(1)
	for _, nf := range notFoundPaths {
		if nf.Count > maxNotFound {
			maxNotFound = nf.Count
		}
	}

	maxUserAgent := int64(1)
	for _, ua := range userAgents {
		if ua.Count > maxUserAgent {
			maxUserAgent = ua.Count
		}
	}

	maxMethod := int64(1)
	for _, m := range methods {
		if m.Count > maxMethod {
			maxMethod = m.Count
		}
	}

	maxStatusDet := int64(1)
	for _, sd := range statusDetails {
		if sd.Count > maxStatusDet {
			maxStatusDet = sd.Count
		}
	}

	maxHourOfDay := int64(1)
	for _, h := range hourOfDay {
		if h.Count > maxHourOfDay {
			maxHourOfDay = h.Count
		}
	}

	maxReferrer := int64(1)
	for _, r := range referrers {
		if r.Count > maxReferrer {
			maxReferrer = r.Count
		}
	}

	// Build status code donut segments
	statusDonutColors := map[string]string{
		"2xx":   "var(--success)",
		"3xx":   "var(--brand)",
		"4xx":   "var(--warning)",
		"5xx":   "var(--error)",
		"other": "var(--text-secondary)",
	}
	var statusDonut []DonutSegment
	for _, sc := range statusCodes {
		color := statusDonutColors[sc.Class]
		if color == "" {
			color = "var(--text-secondary)"
		}
		statusDonut = append(statusDonut, DonutSegment{
			Label: sc.Class,
			Count: sc.Count,
			Color: color,
		})
	}
	computeDonutPositions(statusDonut)

	// Build method donut segments
	methodDonutColors := map[string]string{
		"GET":     "#58a6ff",
		"POST":    "#3fb950",
		"PUT":     "#d29922",
		"DELETE":  "#f85149",
		"PATCH":   "#8b5cf6",
		"HEAD":    "#06b6d4",
		"OPTIONS": "#ec4899",
	}
	var methodDonut []DonutSegment
	for _, m := range methods {
		color := methodDonutColors[m.Method]
		if color == "" {
			color = "#64748b"
		}
		methodDonut = append(methodDonut, DonutSegment{
			Label: m.Method,
			Count: m.Count,
			Color: color,
		})
	}
	computeDonutPositions(methodDonut)

	// Build user agent donut segments
	uaDonutPalette := []string{"#58a6ff", "#3fb950", "#d29922", "#f85149", "#8b5cf6", "#06b6d4", "#ec4899", "#64748b"}
	var userAgentDonut []DonutSegment
	for i, ua := range userAgents {
		color := uaDonutPalette[i%len(uaDonutPalette)]
		userAgentDonut = append(userAgentDonut, DonutSegment{
			Label: ua.Category,
			Count: ua.Count,
			Color: color,
		})
	}
	computeDonutPositions(userAgentDonut)

	// Fetch hour-of-day visitors
	hourVisitors, err := s.queries.HourOfDayVisitors(filter)
	if err != nil {
		log.Printf("Warning: failed to fetch hour visitors: %v", err)
		hourVisitors = nil
	}
	maxHourVisitors := int64(1)
	for _, hv := range hourVisitors {
		if hv.Count > maxHourVisitors {
			maxHourVisitors = hv.Count
		}
	}

	// Fetch path daily trends for sparklines
	pathNames := make([]string, len(topPaths))
	for i, p := range topPaths {
		pathNames[i] = p.Path
	}
	trends, err := s.queries.PathDailyTrends(filter, pathNames)
	if err != nil {
		log.Printf("Warning: failed to fetch path trends: %v", err)
	} else {
		for i := range topPaths {
			if t, ok := trends[topPaths[i].Path]; ok {
				topPaths[i].Trend = t
			}
		}
	}

	// Compute path percentages based on total requests
	if stats != nil && stats.Requests > 0 {
		for i := range topPaths {
			topPaths[i].Pct = float64(topPaths[i].Count) / float64(stats.Requests) * 100
		}
		for i := range notFoundPaths {
			notFoundPaths[i].Pct = float64(notFoundPaths[i].Count) / float64(stats.Requests) * 100
		}
	}

	// Fetch new analytics data
	browsers, err := s.queries.BrowserBreakdown(filter)
	if err != nil {
		log.Printf("Warning: failed to fetch browser breakdown: %v", err)
	}

	osStats, err := s.queries.OSBreakdown(filter)
	if err != nil {
		log.Printf("Warning: failed to fetch OS breakdown: %v", err)
	}

	durationHist, err := s.queries.DurationHistogram(filter)
	if err != nil {
		log.Printf("Warning: failed to fetch duration histogram: %v", err)
	}

	percentiles, err := s.queries.DurationPercentiles(filter)
	if err != nil {
		log.Printf("Warning: failed to fetch duration percentiles: %v", err)
	}

	bandwidthChart, err := s.queries.BandwidthTimeSeries(filter, useDaily)
	if err != nil {
		log.Printf("Warning: failed to fetch bandwidth time series: %v", err)
	}

	responseTimeChart, err := s.queries.ResponseTimeTimeSeries(filter, useDaily)
	if err != nil {
		log.Printf("Warning: failed to fetch response time series: %v", err)
	}

	// Country breakdown (only if GeoIP is configured)
	geoIPEnabled := s.config.GeoIPPath != ""
	var countries []CountryStat
	if geoIPEnabled {
		countries, err = s.queries.CountryBreakdown(filter, 20)
		if err != nil {
			log.Printf("Warning: failed to fetch country breakdown: %v", err)
		}
	}

	// Build browser donut
	browserDonutPalette := []string{"#58a6ff", "#3fb950", "#d29922", "#f85149", "#8b5cf6", "#06b6d4", "#ec4899", "#64748b"}
	var browserDonut []DonutSegment
	for i, b := range browsers {
		browserDonut = append(browserDonut, DonutSegment{
			Label: b.Browser,
			Count: b.Count,
			Color: browserDonutPalette[i%len(browserDonutPalette)],
		})
	}
	computeDonutPositions(browserDonut)

	// Build OS donut
	osDonutPalette := []string{"#58a6ff", "#3fb950", "#d29922", "#f85149", "#8b5cf6", "#06b6d4", "#ec4899", "#64748b"}
	var osDonut []DonutSegment
	for i, o := range osStats {
		osDonut = append(osDonut, DonutSegment{
			Label: o.OS,
			Count: o.Count,
			Color: osDonutPalette[i%len(osDonutPalette)],
		})
	}
	computeDonutPositions(osDonut)

	// Compute mobile vs desktop from OS data
	var mobileCount, desktopCount int64
	for _, o := range osStats {
		switch o.OS {
		case "iOS", "Android":
			mobileCount += o.Count
		case "Windows", "macOS", "Linux", "ChromeOS":
			desktopCount += o.Count
		}
	}
	totalDevices := mobileCount + desktopCount
	mobilePct := 0.0
	desktopPct := 0.0
	if totalDevices > 0 {
		mobilePct = float64(mobileCount) / float64(totalDevices) * 100
		desktopPct = float64(desktopCount) / float64(totalDevices) * 100
	}

	// Max values for new panel bar charts
	maxCountry := int64(1)
	for _, c := range countries {
		if c.Count > maxCountry {
			maxCountry = c.Count
		}
	}

	maxBrowser := int64(1)
	for _, b := range browsers {
		if b.Count > maxBrowser {
			maxBrowser = b.Count
		}
	}

	maxOS := int64(1)
	for _, o := range osStats {
		if o.Count > maxOS {
			maxOS = o.Count
		}
	}

	maxDurationHist := int64(1)
	for _, d := range durationHist {
		if d.Count > maxDurationHist {
			maxDurationHist = d.Count
		}
	}

	maxBandwidth := int64(1)
	for _, b := range bandwidthChart {
		if b.Count > maxBandwidth {
			maxBandwidth = b.Count
		}
	}

	maxResponseTime := int64(1)
	for _, r := range responseTimeChart {
		if r.Count > maxResponseTime {
			maxResponseTime = r.Count
		}
	}

	// Fetch available routers for filter dropdown
	routers, err := s.queries.Routers()
	if err != nil {
		log.Printf("Warning: failed to fetch routers: %v", err)
		routers = []string{}
	}

	return &OverviewData{
		Stats:           stats,
		RequestsChart:   requestsChart,
		VisitorsChart:   visitorsChart,
		TopPaths:        topPaths,
		StatusCodes:     statusCodes,
		TopReferrers:    referrers,
		NotFoundPaths:   notFoundPaths,
		UserAgents:      userAgents,
		Methods:         methods,
		StatusDetails:   statusDetails,
		HourOfDay:       hourOfDay,
		MaxRequests:     maxRequests,
		MaxVisitors:     maxVisitors,
		MaxStatus:       maxStatus,
		MaxNotFound:     maxNotFound,
		MaxUserAgent:    maxUserAgent,
		MaxMethod:       maxMethod,
		MaxStatusDet:    maxStatusDet,
		MaxHourOfDay:    maxHourOfDay,
		MaxReferrer:     maxReferrer,
		Range:           rangeParam,
		CustomFrom:      customFrom,
		CustomTo:        customTo,
		Router:          router,
		IncludeBots:     includeBots,
		Routers:         routers,
		Page:            "overview",
		ActiveTab:       activeTab,
		StatusDonut:     statusDonut,
		MethodDonut:     methodDonut,
		UserAgentDonut:  userAgentDonut,
		HourVisitors:      hourVisitors,
		MaxHourVisitors:   maxHourVisitors,
		Countries:         countries,
		Browsers:          browsers,
		OSStats:           osStats,
		BrowserDonut:      browserDonut,
		OSDonut:           osDonut,
		DurationHist:      durationHist,
		Percentiles:       percentiles,
		BandwidthChart:    bandwidthChart,
		ResponseTimeChart: responseTimeChart,
		MobilePct:         mobilePct,
		DesktopPct:        desktopPct,
		GeoIPEnabled:      geoIPEnabled,
		MaxCountry:        maxCountry,
		MaxBrowser:        maxBrowser,
		MaxOS:             maxOS,
		MaxDurationHist:   maxDurationHist,
		MaxBandwidth:      maxBandwidth,
		MaxResponseTime:   maxResponseTime,
	}, nil
}

// DrilldownPathData represents data for the path drilldown partial
type DrilldownPathData struct {
	Path    string
	Details []PathDetail
}

// DrilldownStatusData represents data for the status drilldown partial
type DrilldownStatusData struct {
	Class    string
	Statuses []SpecificStatusStat
	Max      int64
}

// handlePathDrilldown serves the inline drilldown detail for a path
func (s *Server) handlePathDrilldown(c *fiber.Ctx) error {
	path := c.Query("path")
	if path == "" {
		return c.Status(400).SendString("path parameter required")
	}

	router := c.Query("router", "")
	includeBots := c.Query("bots", "false") == "true"
	filter, _ := s.buildFilterWithCustom(c, router, includeBots)

	details, err := s.queries.PathDrilldown(filter, path)
	if err != nil {
		log.Printf("Error fetching path drilldown: %v", err)
		return c.Status(500).SendString("Error loading drilldown")
	}

	data := DrilldownPathData{
		Path:    path,
		Details: details,
	}

	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, "drilldown_path.html", data); err != nil {
		log.Printf("Error rendering drilldown template: %v", err)
		return c.Status(500).SendString("Error rendering drilldown")
	}

	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.Send(buf.Bytes())
}

// handleStatusDrilldown serves the inline drilldown detail for a status class
func (s *Server) handleStatusDrilldown(c *fiber.Ctx) error {
	class := c.Query("class")
	if class == "" {
		return c.Status(400).SendString("class parameter required")
	}

	router := c.Query("router", "")
	includeBots := c.Query("bots", "false") == "true"
	filter, _ := s.buildFilterWithCustom(c, router, includeBots)

	statuses, err := s.queries.StatusClassDrilldown(filter, class)
	if err != nil {
		log.Printf("Error fetching status drilldown: %v", err)
		return c.Status(500).SendString("Error loading drilldown")
	}

	max := int64(1)
	for _, s := range statuses {
		if s.Count > max {
			max = s.Count
		}
	}

	data := DrilldownStatusData{
		Class:    class,
		Statuses: statuses,
		Max:      max,
	}

	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, "drilldown_status.html", data); err != nil {
		log.Printf("Error rendering status drilldown template: %v", err)
		return c.Status(500).SendString("Error rendering drilldown")
	}

	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.Send(buf.Bytes())
}

// DrilldownStatusCodeData represents data for the status code drilldown partial
type DrilldownStatusCodeData struct {
	Code       int
	Paths      []StatusCodePathStat
	Methods    []StatusCodeMethodStat
	MaxPath    int64
	MaxMethod  int64
}

// handleStatusCodeDrilldown serves the inline drilldown detail for a specific status code
func (s *Server) handleStatusCodeDrilldown(c *fiber.Ctx) error {
	code := c.QueryInt("code", 0)
	if code < 100 || code > 599 {
		return c.Status(400).SendString("invalid status code (must be 100-599)")
	}

	router := c.Query("router", "")
	includeBots := c.Query("bots", "false") == "true"
	filter, _ := s.buildFilterWithCustom(c, router, includeBots)

	paths, err := s.queries.StatusCodePaths(filter, code, 10)
	if err != nil {
		log.Printf("Error fetching status code paths: %v", err)
		return c.Status(500).SendString("Error loading drilldown")
	}

	// Fetch alternate statuses for each path (controlled N+1, max 10 queries)
	for i := range paths {
		alts, err := s.queries.PathAlternateStatuses(filter, paths[i].Path, code)
		if err != nil {
			log.Printf("Warning: failed to fetch alt statuses for %s: %v", paths[i].Path, err)
			continue
		}
		paths[i].AltStatuses = alts
	}

	methods, err := s.queries.StatusCodeMethods(filter, code)
	if err != nil {
		log.Printf("Error fetching status code methods: %v", err)
		return c.Status(500).SendString("Error loading drilldown")
	}

	maxPath := int64(1)
	for _, p := range paths {
		if p.Count > maxPath {
			maxPath = p.Count
		}
	}

	maxMethod := int64(1)
	for _, m := range methods {
		if m.Count > maxMethod {
			maxMethod = m.Count
		}
	}

	data := DrilldownStatusCodeData{
		Code:      code,
		Paths:     paths,
		Methods:   methods,
		MaxPath:   maxPath,
		MaxMethod: maxMethod,
	}

	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, "drilldown_status_code.html", data); err != nil {
		log.Printf("Error rendering status code drilldown template: %v", err)
		return c.Status(500).SendString("Error rendering drilldown")
	}

	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.Send(buf.Bytes())
}

// PanelPathsData represents data for the paginated paths panel
type PanelPathsData struct {
	Paths      []PathStat
	TotalCount int64
	Page       int
	TotalPages int
	Limit      int
	Sort       string
	Order      string
	Range      string
	Router     string
	IncludeBots bool
	TotalReqs  int64
	Summary    *PathsSummaryResult
}

// PanelReferrersData represents data for the paginated referrers panel
type PanelReferrersData struct {
	Referrers   []ReferrerStat
	TotalCount  int64
	Page        int
	TotalPages  int
	Limit       int
	Sort        string
	Order       string
	Range       string
	Router      string
	IncludeBots bool
	MaxReferrer int64
}

// PanelNotFoundData represents data for the paginated 404 panel
type PanelNotFoundData struct {
	Paths      []PathStat
	TotalCount int64
	Page       int
	TotalPages int
	Limit      int
	Sort       string
	Order      string
	Range      string
	Router     string
	IncludeBots bool
}

// handlePanelPaths serves the paginated paths panel
func (s *Server) handlePanelPaths(c *fiber.Ctx) error {
	router := c.Query("router", "")
	includeBots := c.Query("bots", "false") == "true"
	filter, rangeParam := s.buildFilterWithCustom(c, router, includeBots)

	page := c.QueryInt("page", 1)
	limit := c.QueryInt("limit", 10)
	sort := c.Query("sort", "count")
	order := c.Query("order", "desc")

	result, err := s.queries.TopPathsPaginated(filter, page, limit, sort, order)
	if err != nil {
		log.Printf("Error fetching paginated paths: %v", err)
		return c.Status(500).SendString("Error loading paths")
	}

	summary, err := s.queries.PathsSummary(filter)
	if err != nil {
		log.Printf("Warning: failed to fetch paths summary: %v", err)
	}

	stats, _ := s.queries.TotalStats(filter)
	totalReqs := int64(0)
	if stats != nil {
		totalReqs = stats.Requests
	}

	items := result.Items.([]PathStat)
	if totalReqs > 0 {
		for i := range items {
			items[i].Pct = float64(items[i].Count) / float64(totalReqs) * 100
		}
	}

	data := PanelPathsData{
		Paths:       items,
		TotalCount:  result.TotalCount,
		Page:        result.Page,
		TotalPages:  result.TotalPages,
		Limit:       limit,
		Sort:        sort,
		Order:       order,
		Range:       rangeParam,
		Router:      router,
		IncludeBots: includeBots,
		TotalReqs:   totalReqs,
		Summary:     summary,
	}

	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, "panel_paths.html", data); err != nil {
		log.Printf("Error rendering panel template: %v", err)
		return c.Status(500).SendString("Error rendering panel")
	}

	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.Send(buf.Bytes())
}

// handlePanelReferrers serves the paginated referrers panel
func (s *Server) handlePanelReferrers(c *fiber.Ctx) error {
	router := c.Query("router", "")
	includeBots := c.Query("bots", "false") == "true"
	filter, rangeParam := s.buildFilterWithCustom(c, router, includeBots)

	limit := c.QueryInt("limit", 10)

	referrers, err := s.queries.TopReferrers(filter, limit)
	if err != nil {
		log.Printf("Error fetching referrers: %v", err)
		return c.Status(500).SendString("Error loading referrers")
	}

	maxRef := int64(1)
	for _, r := range referrers {
		if r.Count > maxRef {
			maxRef = r.Count
		}
	}

	data := PanelReferrersData{
		Referrers:   referrers,
		Range:       rangeParam,
		Router:      router,
		IncludeBots: includeBots,
		MaxReferrer: maxRef,
	}

	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, "panel_referrers.html", data); err != nil {
		log.Printf("Error rendering referrers panel: %v", err)
		return c.Status(500).SendString("Error rendering panel")
	}

	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.Send(buf.Bytes())
}

// handlePanelNotFound serves the paginated 404 panel
func (s *Server) handlePanelNotFound(c *fiber.Ctx) error {
	router := c.Query("router", "")
	includeBots := c.Query("bots", "false") == "true"
	filter, rangeParam := s.buildFilterWithCustom(c, router, includeBots)

	limit := c.QueryInt("limit", 10)

	notFound, err := s.queries.TopNotFound(filter, limit)
	if err != nil {
		log.Printf("Error fetching 404 paths: %v", err)
		return c.Status(500).SendString("Error loading 404 paths")
	}

	data := PanelNotFoundData{
		Paths:       notFound,
		Range:       rangeParam,
		Router:      router,
		IncludeBots: includeBots,
	}

	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, "panel_not_found.html", data); err != nil {
		log.Printf("Error rendering not-found panel: %v", err)
		return c.Status(500).SendString("Error rendering panel")
	}

	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.Send(buf.Bytes())
}

// buildFilter constructs a Filter based on the range parameter
func (s *Server) buildFilter(rangeParam, router string, includeBots bool) Filter {
	now := time.Now().UTC()
	var from, to time.Time

	switch rangeParam {
	case "7d":
		from = now.AddDate(0, 0, -7).Truncate(24 * time.Hour)
		to = now.Truncate(time.Hour)
	case "30d":
		from = now.AddDate(0, 0, -30).Truncate(24 * time.Hour)
		to = now.Truncate(time.Hour)
	default: // "today"
		from = now.Truncate(24 * time.Hour)
		to = now.Truncate(time.Hour)
	}

	return Filter{
		From:        from.Format(time.RFC3339),
		To:          to.Format(time.RFC3339),
		Router:      router,
		IncludeBots: includeBots,
	}
}

// buildFilterWithCustom extends buildFilter with custom date range support
func (s *Server) buildFilterWithCustom(c *fiber.Ctx, router string, includeBots bool) (Filter, string) {
	rangeParam := c.Query("range", "today")
	customFrom := c.Query("custom_from", "")
	customTo := c.Query("custom_to", "")

	if rangeParam == "custom" && customFrom != "" && customTo != "" {
		fromTime, errFrom := time.Parse("2006-01-02", customFrom)
		toTime, errTo := time.Parse("2006-01-02", customTo)
		if errFrom == nil && errTo == nil && fromTime.Before(toTime) {
			// Cap at 365 days
			if toTime.Sub(fromTime) > 365*24*time.Hour {
				fromTime = toTime.AddDate(0, 0, -365)
			}
			return Filter{
				From:        fromTime.Format(time.RFC3339),
				To:          toTime.Add(23*time.Hour + 59*time.Minute).Format(time.RFC3339),
				Router:      router,
				IncludeBots: includeBots,
			}, rangeParam
		}
	}

	return s.buildFilter(rangeParam, router, includeBots), rangeParam
}

// validSecurityTabs is the set of valid tab names for the security page
var validSecurityTabs = map[string]bool{
	"summary":     true,
	"errors":      true,
	"performance": true,
}

// handleAPISecurityPartial serves the htmx partial for security data
func (s *Server) handleAPISecurityPartial(c *fiber.Ctx) error {
	data, err := s.getSecurityData(c)
	if err != nil {
		log.Printf("Error loading security data: %v", err)
		return c.Status(500).SendString("Error loading security data")
	}

	tmplName := "security_tab_" + data.ActiveTab + ".html"

	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, tmplName, data); err != nil {
		log.Printf("Error rendering security tab template %s: %v", tmplName, err)
		return c.Status(500).SendString("Error rendering partial")
	}

	c.Set("Content-Type", "text/html; charset=utf-8")
	return c.Send(buf.Bytes())
}

// getSecurityData fetches and prepares data for the security page
func (s *Server) getSecurityData(c *fiber.Ctx) (*SecurityData, error) {
	// Parse active tab
	activeTab := c.Query("tab", "summary")
	if !validSecurityTabs[activeTab] {
		activeTab = "summary"
	}

	// Build filter with IncludeBots=true (security view shows all traffic)
	filter, rangeParam := s.buildFilterWithCustom(c, "", true)
	if rangeParam == "today" {
		// Default security to 30d if no explicit range
		defaultRange := c.Query("range", "30d")
		if defaultRange == "30d" {
			filter = s.buildFilter("30d", "", true)
			rangeParam = "30d"
		}
	}
	customFrom := c.Query("custom_from", "")
	customTo := c.Query("custom_to", "")

	// Threat patterns - use suspicious path mode for combined format
	suspiciousPathMode := s.config.LogFormat == "combined"
	threatPatterns, err := s.queries.ThreatPatterns(filter, suspiciousPathMode)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch threat patterns: %w", err)
	}

	totalUnrouted := int64(0)
	maxThreat := int64(1)
	for _, tp := range threatPatterns {
		totalUnrouted += tp.Count
		if tp.Count > maxThreat {
			maxThreat = tp.Count
		}
	}

	// Bot vs Human
	humanCount, botCount, botBreakdown, err := s.queries.BotVsHuman(filter)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch bot breakdown: %w", err)
	}

	botPct := float64(0)
	humanPct := float64(0)
	totalTraffic := humanCount + botCount
	if totalTraffic > 0 {
		botPct = float64(botCount) / float64(totalTraffic) * 100
		humanPct = 100.0 - botPct
	}

	// Compute percentages for bot breakdown
	for i := range botBreakdown {
		if botCount > 0 {
			botBreakdown[i].Pct = float64(botBreakdown[i].Count) / float64(botCount) * 100
		}
	}

	// Error trends
	errorTrends, err := s.queries.ErrorTrends(filter)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch error trends: %w", err)
	}

	total5xx := int64(0)
	maxErrorCount := int64(1)
	for _, et := range errorTrends {
		total5xx += et.Count
		if et.Count > maxErrorCount {
			maxErrorCount = et.Count
		}
	}

	// Error paths
	errorPaths, err := s.queries.ErrorPaths(filter, 10)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch error paths: %w", err)
	}

	// Slowest paths
	slowestPaths, err := s.queries.SlowestPaths(filter, 10)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch slowest paths: %w", err)
	}

	slowestAvgMs := int64(0)
	if len(slowestPaths) > 0 {
		slowestAvgMs = slowestPaths[0].AvgMs
	}

	return &SecurityData{
		TotalUnrouted:  totalUnrouted,
		BotPct:         botPct,
		HumanPct:       humanPct,
		Total5xx:       total5xx,
		SlowestAvgMs:   slowestAvgMs,
		ThreatPatterns: threatPatterns,
		BotBreakdown:   botBreakdown,
		HumanCount:     humanCount,
		BotCount:       botCount,
		TotalTraffic:   totalTraffic,
		MaxThreat:      maxThreat,
		ErrorTrends:    errorTrends,
		MaxErrorCount:  maxErrorCount,
		ErrorPaths:     errorPaths,
		SlowestPaths:   slowestPaths,
		Range:          rangeParam,
		CustomFrom:     customFrom,
		CustomTo:       customTo,
		Page:           "security",
		ActiveTab:      activeTab,
	}, nil
}
