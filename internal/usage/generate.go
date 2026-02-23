package usage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/derekurban/profilex-cli/internal/store"
)

func GenerateBundle(ctx context.Context, st *store.State, statePath string, opts GenerateOptions) (*UnifiedLocalBundle, error) {
	if opts.MaxFiles <= 0 {
		opts.MaxFiles = 5000
	}
	if opts.Timezone == "" {
		opts.Timezone = time.Now().Location().String()
	}
	if opts.CostMode == "" {
		opts.CostMode = CostModeAuto
	}

	notes := []string{}

	roots := discoverRoots(opts.RootDir, st)
	if len(roots) == 0 {
		notes = append(notes, "No usage roots found in default locations")
	}

	files, err := collectJSONLFiles(roots, opts.Deep, opts.MaxFiles)
	if err != nil {
		return nil, err
	}

	pricing, err := fetchPricingCatalog(ctx)
	pricingLoaded := err == nil
	if err != nil {
		notes = append(notes, fmt.Sprintf("Pricing catalog unavailable: %v", err))
	} else {
		notes = append(notes, fmt.Sprintf("Loaded pricing catalog (%d rows)", len(pricing)))
	}

	resolver := newProfileResolver(st)
	claudeSeen := map[string]bool{}
	events := make([]NormalizedEvent, 0)
	malformedLines := 0
	zeroFiles := 0
	parseFailures := 0

	for _, file := range files {
		rows, malformed, err := parseUsageFile(file, resolver, opts, pricing, claudeSeen)
		malformedLines += malformed
		if err != nil {
			parseFailures++
			notes = append(notes, fmt.Sprintf("Failed to parse %s: %v", file, err))
			continue
		}
		if len(rows) == 0 {
			zeroFiles++
		} else {
			events = append(events, rows...)
		}
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].TimestampUTC < events[j].TimestampUTC
	})

	notes = append(notes, fmt.Sprintf("Files with zero parsed events: %d", zeroFiles))
	notes = append(notes, fmt.Sprintf("Files with read/parse failures: %d", parseFailures))
	notes = append(notes, fmt.Sprintf("Malformed JSONL lines skipped: %d", malformedLines))

	bundle := &UnifiedLocalBundle{
		SchemaVersion:  1,
		GeneratedAtUTC: time.Now().UTC().Format(time.RFC3339),
		Timezone:       opts.Timezone,
		CostMode:       opts.CostMode,
		PricingLoaded:  pricingLoaded,
		ProfilexState:  st,
		Events:         events,
		Source: UnifiedSourceSummary{
			ProfilexStatePath: normalizePath(statePath),
			UsageRoots:        roots,
			UsageFiles:        files,
		},
		Notes: notes,
	}

	return bundle, nil
}

func WriteBundle(path string, bundle *UnifiedLocalBundle) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	return os.WriteFile(path, b, 0o644)
}
