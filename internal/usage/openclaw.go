package usage

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"sort"
	"strings"
	"time"
)

func collectOpenClawEvents(ctx context.Context, timezone string) ([]NormalizedEvent, []string, error) {
	if _, err := exec.LookPath("openclaw"); err != nil {
		return nil, []string{"openclaw binary not found in PATH; skipping OpenClaw usage import"}, nil
	}

	cmd := exec.CommandContext(ctx, "openclaw", "status", "--json", "--usage")
	out, err := cmd.Output()
	if err != nil {
		// Soft-fail: usage export should still succeed without OpenClaw data.
		return nil, []string{fmt.Sprintf("openclaw status --json --usage failed: %v", err)}, nil
	}

	var payload map[string]any
	if err := json.Unmarshal(out, &payload); err != nil {
		return nil, []string{fmt.Sprintf("openclaw usage payload parse failed: %v", err)}, nil
	}

	usage := getMap(payload, "usage")
	if usage == nil {
		return nil, []string{"openclaw usage payload has no usage section"}, nil
	}

	rawRows := []openclawRow{}
	walkOpenClawUsage(usage, "", &rawRows)
	if len(rawRows) == 0 {
		return nil, []string{"openclaw usage payload had no parsable session rows"}, nil
	}

	// Deduplicate stable rows.
	seen := map[string]bool{}
	events := make([]NormalizedEvent, 0, len(rawRows))
	for i, row := range rawRows {
		k := row.provider + "|" + row.sessionKey + "|" + row.updatedAt + "|" + fmt.Sprintf("%d|%d|%.8f", row.in, row.out, row.cost)
		if seen[k] {
			continue
		}
		seen[k] = true

		ts := row.updatedAt
		if strings.TrimSpace(ts) == "" {
			ts = time.Now().UTC().Format(time.RFC3339)
		}
		provider := row.provider
		if provider == "" {
			provider = "unknown"
		}
		sessionKey := row.sessionKey
		if sessionKey == "" {
			sessionKey = fmt.Sprintf("recent-%d", i+1)
		}

		events = append(events, NormalizedEvent{
			ID:                    fmt.Sprintf("openclaw-%s-%s-%d", sanitize(provider), sanitize(sessionKey), i+1),
			TimestampUTC:          ts,
			DateLocal:             dateLocal(ts, timezone),
			Tool:                  ToolOpenClaw,
			ProfileID:             "openclaw/" + sanitize(provider),
			ProfileName:           provider,
			IsProfilexManaged:     false,
			SourceRoot:            "openclaw-status",
			SourceFile:            "openclaw:status",
			SessionID:             sessionKey,
			Project:               "",
			Model:                 row.model,
			IsFallbackModel:       false,
			InputTokens:           row.in,
			CachedInputTokens:     0,
			OutputTokens:          row.out,
			ReasoningOutputTokens: 0,
			CacheCreationTokens:   0,
			CacheReadTokens:       0,
			RawTotalTokens:        row.in + row.out,
			NormalizedTotalTokens: row.in + row.out,
			ObservedCostUSD:       row.cost,
			CalculatedCostUSD:     row.cost,
			EffectiveCostUSD:      row.cost,
			CostModeUsed:          CostModeDisplay,
		})
	}

	sort.Slice(events, func(i, j int) bool {
		return events[i].TimestampUTC < events[j].TimestampUTC
	})

	return events, []string{fmt.Sprintf("openclaw usage imported %d session rows", len(events))}, nil
}

type openclawRow struct {
	provider   string
	sessionKey string
	label      string
	kind       string
	agentID    string
	model      string
	updatedAt  string
	in         int64
	out        int64
	cost       float64
}

func walkOpenClawUsage(node any, provider string, out *[]openclawRow) {
	switch v := node.(type) {
	case map[string]any:
		// Provider branch support: usage.providers.<provider>
		if providers, ok := v["providers"].(map[string]any); ok {
			for pName, pNode := range providers {
				walkOpenClawUsage(pNode, pName, out)
			}
		}

		row, ok := asOpenClawRow(v, provider)
		if ok {
			*out = append(*out, row)
		}

		for _, child := range v {
			walkOpenClawUsage(child, provider, out)
		}
	case []any:
		for _, item := range v {
			walkOpenClawUsage(item, provider, out)
		}
	}
}

func asOpenClawRow(m map[string]any, providerHint string) (openclawRow, bool) {
	in := int64(getFloatAny(m, "in", "input", "inputTokens", "input_tokens", "inTokens"))
	outTok := int64(getFloatAny(m, "out", "output", "outputTokens", "output_tokens", "outTokens"))
	cost := getFloatAny(m, "costUsd", "costUSD", "cost_usd", "cost")

	sessionKey := getStringAny(m, "sessionKey", "session_key", "session", "key")
	updatedAt := getStringAny(m, "updatedAt", "updated_at", "timestamp", "time")
	model := getStringAny(m, "model")

	// Require row-like signature so we don't parse aggregate summaries as sessions.
	hasSessionish := sessionKey != "" || updatedAt != "" || model != ""
	hasUsageish := in > 0 || outTok > 0 || cost > 0
	if !hasSessionish || !hasUsageish {
		return openclawRow{}, false
	}

	provider := firstNonEmpty(getStringAny(m, "provider", "providerName"), providerHint)
	label := getStringAny(m, "label", "sessionLabel")
	kind := getStringAny(m, "kind", "type")
	agentID := getStringAny(m, "agentId", "agentID")

	return openclawRow{
		provider:   provider,
		sessionKey: sessionKey,
		label:      label,
		kind:       kind,
		agentID:    agentID,
		model:      model,
		updatedAt:  updatedAt,
		in:         in,
		out:        outTok,
		cost:       cost,
	}, true
}

func sanitize(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return "x"
	}
	repl := strings.NewReplacer(" ", "-", "/", "-", "\\", "-", ":", "-", "|", "-", "\t", "-")
	s = repl.Replace(s)
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	s = strings.Trim(s, "-")
	if s == "" {
		return "x"
	}
	return s
}
