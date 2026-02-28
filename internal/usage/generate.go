package usage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
		rows, malformed, err := parseUsageFile(file.ParsePath, resolver, opts, pricing, claudeSeen)
		malformedLines += malformed
		if err != nil {
			parseFailures++
			path := file.CanonicalPath
			if strings.TrimSpace(path) == "" {
				path = file.ParsePath
			}
			notes = append(notes, fmt.Sprintf("Failed to parse %s: %v", path, err))
			continue
		}
		if len(rows) == 0 {
			zeroFiles++
		} else {
			rows = annotateSharedMetadata(rows, file, st)
			events = append(events, rows...)
		}
	}

	aliasCount := 0
	collapsedAliases := 0
	sharedEventCount := 0
	for _, file := range files {
		aliasCount += len(file.AliasPaths)
		if len(file.AliasPaths) > 1 {
			collapsedAliases += len(file.AliasPaths) - 1
		}
	}
	for i := range events {
		if events[i].IsSharedSession {
			sharedEventCount++
		}
	}
	if collapsedAliases > 0 {
		notes = append(notes, fmt.Sprintf("Canonicalized %d JSONL aliases to avoid duplicate counting across shared links", collapsedAliases))
	}
	if sharedEventCount > 0 {
		notes = append(notes, fmt.Sprintf("Events flagged as shared sessions: %d", sharedEventCount))
	}
	if aliasCount > 0 && len(files) > 0 {
		notes = append(notes, fmt.Sprintf("Canonical usage files parsed: %d (discovered aliases: %d)", len(files), aliasCount))
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].TimestampUTC < events[j].TimestampUTC
	})

	notes = append(notes, fmt.Sprintf("Files with zero parsed events: %d", zeroFiles))
	notes = append(notes, fmt.Sprintf("Files with read/parse failures: %d", parseFailures))
	notes = append(notes, fmt.Sprintf("Malformed JSONL lines skipped: %d", malformedLines))

	openclawEvents, openclawNotes, err := collectOpenClawEvents(ctx, opts.Timezone)
	if err != nil {
		notes = append(notes, fmt.Sprintf("OpenClaw usage collection error: %v", err))
	} else {
		if len(openclawEvents) > 0 {
			events = append(events, openclawEvents...)
		}
		notes = append(notes, openclawNotes...)
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].TimestampUTC < events[j].TimestampUTC
	})

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
			UsageFiles:        usageFilePaths(files),
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

func usageFilePaths(files []usageFile) []string {
	out := make([]string, 0, len(files))
	for _, f := range files {
		p := f.CanonicalPath
		if strings.TrimSpace(p) == "" {
			p = f.ParsePath
		}
		out = append(out, p)
	}
	return out
}

func annotateSharedMetadata(rows []NormalizedEvent, file usageFile, st *store.State) []NormalizedEvent {
	if len(rows) == 0 {
		return rows
	}

	tool := rows[0].Tool
	contributors := managedContributorsForRoots(st, tool, file.AliasRoots)
	shared := isSharedSessionFile(file, tool, contributors)
	if !shared {
		return rows
	}

	ids := make([]string, 0, len(contributors))
	names := make([]string, 0, len(contributors))
	for _, c := range contributors {
		ids = append(ids, c.id)
		names = append(names, c.name)
	}

	for i := range rows {
		rows[i].IsSharedSession = true
		rows[i].SharedSessionProfileIDs = ids
		rows[i].SharedSessionProfileNames = names
		rows[i].SharedSessionSources = file.AliasPaths

		if len(contributors) == 1 {
			rows[i].ProfileID = contributors[0].id
			rows[i].ProfileName = contributors[0].name
			rows[i].IsProfilexManaged = true
			continue
		}
		if len(contributors) > 1 {
			rows[i].ProfileID = string(rows[i].Tool) + "/shared"
			rows[i].ProfileName = "shared"
			rows[i].IsProfilexManaged = false
		}
	}
	return rows
}

type contributingProfile struct {
	id   string
	name string
}

func managedContributorsForRoots(st *store.State, tool Tool, roots []string) []contributingProfile {
	if st == nil || len(roots) == 0 {
		return nil
	}

	leaf, ok := sessionLeafForUsageTool(tool)
	if !ok {
		return nil
	}

	rootSet := map[string]bool{}
	for _, root := range roots {
		rootSet[pathKey(root)] = true
	}

	out := make([]contributingProfile, 0)
	seen := map[string]bool{}
	for _, p := range st.Profiles {
		if Tool(p.Tool) != tool {
			continue
		}
		profileRoot := ensureLeaf(p.Dir, leaf)
		if !rootSet[pathKey(profileRoot)] {
			continue
		}
		id := string(p.Tool) + "/" + p.Name
		if seen[id] {
			continue
		}
		seen[id] = true
		out = append(out, contributingProfile{id: id, name: p.Name})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].id < out[j].id
	})
	return out
}

func isSharedSessionFile(file usageFile, tool Tool, contributors []contributingProfile) bool {
	if len(contributors) > 1 {
		return true
	}

	leaf, ok := sessionLeafForUsageTool(tool)
	if ok {
		marker := "/shared/" + strings.ToLower(string(tool)) + "/" + strings.ToLower(leaf)
		for _, root := range file.AliasRoots {
			if strings.Contains(strings.ToLower(normalizePath(root)), marker) {
				return true
			}
		}
		path := file.CanonicalPath
		if strings.TrimSpace(path) == "" {
			path = file.ParsePath
		}
		if strings.Contains(strings.ToLower(normalizePath(path)), marker) {
			return true
		}
	}

	return len(file.AliasPaths) > 1
}

func sessionLeafForUsageTool(tool Tool) (string, bool) {
	switch tool {
	case ToolClaude:
		return "projects", true
	case ToolCodex:
		return "sessions", true
	default:
		return "", false
	}
}
