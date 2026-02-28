package usage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type parsedEntry struct {
	Obj       map[string]any
	LineIndex int
}

type totals struct {
	Input     int64
	Cached    int64
	Output    int64
	Reasoning int64
	Total     int64
}

func parseUsageFile(path string, resolver *profileResolver, opts GenerateOptions, pricing pricingCatalog, claudeSeen map[string]bool) ([]NormalizedEvent, int, error) {
	entries, malformed, err := flattenJSONL(path)
	if err != nil {
		return nil, malformed, err
	}
	if len(entries) == 0 {
		return nil, malformed, nil
	}

	tool := inferTool(path, entries, opts)
	root := extractRootFromFile(path, tool)

	switch tool {
	case ToolCodex:
		rows := normalizeCodex(path, root, entries, resolver, opts, pricing)
		return rows, malformed, nil
	default:
		rows := normalizeClaude(path, root, entries, resolver, opts, pricing, claudeSeen)
		return rows, malformed, nil
	}
}

func flattenJSONL(path string) ([]parsedEntry, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	entries := []parsedEntry{}
	malformed := 0
	reader := bufio.NewReader(f)
	lineIdx := 0
	for {
		raw, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return nil, malformed, err
		}

		line := strings.TrimSpace(raw)
		if line == "" {
			if err == io.EOF && raw == "" {
				break
			}
			lineIdx++
			if err == io.EOF {
				break
			}
			continue
		}
		objs, ok := parseLineObjects(line)
		if !ok {
			malformed++
			lineIdx++
			if err == io.EOF {
				break
			}
			continue
		}
		for _, obj := range objs {
			entries = append(entries, parsedEntry{Obj: obj, LineIndex: lineIdx})
		}
		lineIdx++
		if err == io.EOF {
			break
		}
	}
	return entries, malformed, nil
}

func parseLineObjects(line string) ([]map[string]any, bool) {
	line = strings.TrimSpace(strings.TrimPrefix(line, "\uFEFF"))
	if line == "" {
		return nil, true
	}
	var v any
	if err := json.Unmarshal([]byte(line), &v); err != nil {
		return nil, false
	}
	if m, ok := v.(map[string]any); ok {
		return []map[string]any{m}, true
	}
	if arr, ok := v.([]any); ok {
		out := []map[string]any{}
		for _, item := range arr {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out, true
	}
	return nil, false
}

func inferTool(filePath string, entries []parsedEntry, opts GenerateOptions) Tool {
	_ = opts
	codex := 0
	claude := 0
	max := len(entries)
	if max > 300 {
		max = 300
	}
	for i := 0; i < max; i++ {
		t := detectLineTool(entries[i].Obj)
		if t == ToolCodex {
			codex++
		}
		if t == ToolClaude {
			claude++
		}
	}
	if codex == 0 && claude == 0 {
		return inferToolFromPath(filePath)
	}
	if codex >= claude {
		return ToolCodex
	}
	return ToolClaude
}

func detectLineTool(obj map[string]any) Tool {
	typeV := strings.ToLower(getString(obj, "type"))
	payload := getMap(obj, "payload")
	payloadType := strings.ToLower(getString(payload, "type"))
	if typeV == "session_meta" || typeV == "turn_context" || typeV == "response_item" {
		return ToolCodex
	}
	if typeV == "event_msg" {
		switch payloadType {
		case "token_count", "user_message", "agent_message", "agent_reasoning":
			return ToolCodex
		}
	}
	info := getMap(payload, "info")
	if info != nil && (info["total_token_usage"] != nil || info["last_token_usage"] != nil) {
		return ToolCodex
	}
	msg := getMap(obj, "message")
	if msg != nil {
		if getMap(msg, "usage") != nil {
			return ToolClaude
		}
	}
	if obj["requestId"] != nil || obj["costUSD"] != nil || obj["sessionId"] != nil {
		return ToolClaude
	}
	return ToolUnknown
}

func normalizeClaude(path, root string, entries []parsedEntry, resolver *profileResolver, opts GenerateOptions, pricing pricingCatalog, seen map[string]bool) []NormalizedEvent {
	rows := []NormalizedEvent{}
	pm := resolver.resolve(ToolClaude, root)
	for _, e := range entries {
		obj := e.Obj
		msg := getMap(obj, "message")
		usage := getMap(msg, "usage")
		if usage == nil {
			usage = getMap(obj, "usage")
		}
		if usage == nil {
			usage = getMap(getMap(obj, "result"), "usage")
		}
		if usage == nil {
			usage = getMap(getMap(obj, "response"), "usage")
		}
		if usage == nil {
			continue
		}

		dedupe := dedupeClaude(obj)
		if dedupe != "" {
			if seen[dedupe] {
				continue
			}
			seen[dedupe] = true
		}

		in := getInt64Any(usage, "input_tokens", "inputTokens")
		out := getInt64Any(usage, "output_tokens", "outputTokens")
		cacheCreate := getInt64Any(usage, "cache_creation_input_tokens", "cacheCreationInputTokens", "cache_creation_tokens")
		cacheRead := getInt64Any(usage, "cache_read_input_tokens", "cacheReadInputTokens", "cache_read_tokens")
		total := in + out + cacheCreate + cacheRead
		obsCost := getFloatAny(obj, "costUSD", "cost_usd", "cost")
		if total <= 0 && obsCost <= 0 {
			continue
		}

		ts := firstNonEmpty(
			getStringAny(obj, "timestamp", "created_at", "createdAt", "time", "datetime"),
			getStringAny(msg, "timestamp", "created_at", "createdAt"),
			time.Now().UTC().Format(time.RFC3339),
		)
		model := firstNonEmpty(getStringAny(msg, "model", "model_name"), getStringAny(obj, "model", "model_name", "modelName"))

		rec, ok := resolvePricing(pricing, model, ToolClaude)
		calc := 0.0
		if ok {
			calc = calculateCost(ToolClaude, rec, in, 0, out, cacheCreate, cacheRead)
		}
		effective := calc
		switch opts.CostMode {
		case CostModeDisplay:
			effective = obsCost
		case CostModeCalculate:
			effective = calc
		default:
			if obsCost > 0 {
				effective = obsCost
			}
		}

		rows = append(rows, NormalizedEvent{
			ID:                    fmt.Sprintf("claude-%d-%d", e.LineIndex, len(rows)+1),
			TimestampUTC:          ts,
			DateLocal:             dateLocal(ts, opts.Timezone),
			Tool:                  ToolClaude,
			ProfileID:             pm.ProfileID,
			ProfileName:           pm.ProfileName,
			IsProfilexManaged:     pm.IsProfilexManaged,
			SourceRoot:            root,
			SourceFile:            normalizePath(path),
			SessionID:             firstNonEmpty(getStringAny(obj, "sessionId", "session_id"), strings.TrimSuffix(filepath.Base(path), ".jsonl")),
			Project:               firstNonEmpty(getProjectFromCwd(getString(obj, "cwd")), parentName(path, 2)),
			Model:                 model,
			InputTokens:           in,
			CachedInputTokens:     0,
			OutputTokens:          out,
			ReasoningOutputTokens: 0,
			CacheCreationTokens:   cacheCreate,
			CacheReadTokens:       cacheRead,
			RawTotalTokens:        total,
			NormalizedTotalTokens: total,
			ObservedCostUSD:       obsCost,
			CalculatedCostUSD:     calc,
			EffectiveCostUSD:      effective,
			CostModeUsed:          opts.CostMode,
		})
	}
	return rows
}

func normalizeCodex(path, root string, entries []parsedEntry, resolver *profileResolver, opts GenerateOptions, pricing pricingCatalog) []NormalizedEvent {
	rows := []NormalizedEvent{}
	pm := resolver.resolve(ToolCodex, root)

	var prev *totals
	currentModel := ""
	fallbackActive := false

	for _, e := range entries {
		obj := e.Obj
		typeV := strings.ToLower(getString(obj, "type"))
		payload := getMap(obj, "payload")

		if typeV == "turn_context" || typeV == "session_meta" || typeV == "response_item" {
			m := firstNonEmpty(extractModel(payload), extractModel(obj))
			if m != "" {
				currentModel = m
				fallbackActive = false
			}
			continue
		}
		if typeV != "event_msg" || strings.ToLower(getString(payload, "type")) != "token_count" {
			continue
		}

		info := getMap(payload, "info")
		last := extractUsage(getAny(info, "last_token_usage"), getAny(payload, "last_token_usage"))
		total := extractUsage(getAny(info, "total_token_usage"), getAny(payload, "total_token_usage"))

		var u *totals
		if last != nil {
			u = last
		} else if total != nil {
			u = deltaTotals(total, prev)
		}
		if u == nil {
			continue
		}
		if total != nil {
			prev = total
		}

		if u.Input == 0 && u.Cached == 0 && u.Output == 0 && u.Reasoning == 0 {
			continue
		}

		m := firstNonEmpty(extractModel(map[string]any{"payload": payload, "info": info}), extractModel(info), currentModel)
		fallback := false
		if m == "" {
			m = "gpt-5"
			fallback = true
			fallbackActive = true
		} else if fallbackActive {
			fallback = true
		} else {
			fallbackActive = false
		}
		currentModel = m

		ts := firstNonEmpty(getStringAny(obj, "timestamp", "created_at", "createdAt"), getString(payload, "timestamp"), time.Now().UTC().Format(time.RFC3339))
		cached := u.Cached
		if cached > u.Input {
			cached = u.Input
		}
		totalTokens := u.Total
		if totalTokens == 0 {
			totalTokens = u.Input + u.Output
		}

		rec, ok := resolvePricing(pricing, m, ToolCodex)
		calc := 0.0
		if ok {
			calc = calculateCost(ToolCodex, rec, u.Input, cached, u.Output, 0, 0)
		}
		effective := 0.0
		switch opts.CostMode {
		case CostModeDisplay:
			effective = 0
		default:
			effective = calc
		}

		rows = append(rows, NormalizedEvent{
			ID:                    fmt.Sprintf("codex-%d-%d", e.LineIndex, len(rows)+1),
			TimestampUTC:          ts,
			DateLocal:             dateLocal(ts, opts.Timezone),
			Tool:                  ToolCodex,
			ProfileID:             pm.ProfileID,
			ProfileName:           pm.ProfileName,
			IsProfilexManaged:     pm.IsProfilexManaged,
			SourceRoot:            root,
			SourceFile:            normalizePath(path),
			SessionID:             firstNonEmpty(getStringAny(obj, "session_id", "sessionId"), getStringAny(payload, "session_id", "sessionId"), strings.TrimSuffix(filepath.Base(path), ".jsonl")),
			Project:               parentName(path, 3),
			Model:                 m,
			IsFallbackModel:       fallback,
			InputTokens:           u.Input,
			CachedInputTokens:     cached,
			OutputTokens:          u.Output,
			ReasoningOutputTokens: u.Reasoning,
			CacheCreationTokens:   0,
			CacheReadTokens:       0,
			RawTotalTokens:        u.Total,
			NormalizedTotalTokens: totalTokens,
			ObservedCostUSD:       0,
			CalculatedCostUSD:     calc,
			EffectiveCostUSD:      effective,
			CostModeUsed:          opts.CostMode,
		})
	}
	return rows
}

func extractUsage(values ...any) *totals {
	for _, v := range values {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}
		input := getInt64Any(m, "input_tokens", "inputTokens")
		cached := getInt64Any(m, "cached_input_tokens", "cache_read_input_tokens", "cachedInputTokens")
		output := getInt64Any(m, "output_tokens", "outputTokens")
		reasoning := getInt64Any(m, "reasoning_output_tokens", "reasoningOutputTokens")
		total := getInt64Any(m, "total_tokens", "totalTokens")
		if total == 0 {
			total = input + output
		}
		if input == 0 && cached == 0 && output == 0 && total == 0 {
			continue
		}
		return &totals{Input: input, Cached: cached, Output: output, Reasoning: reasoning, Total: total}
	}
	return nil
}

func deltaTotals(curr, prev *totals) *totals {
	if curr == nil {
		return nil
	}
	if prev == nil {
		cp := *curr
		return &cp
	}
	return &totals{
		Input:     max64(curr.Input-prev.Input, 0),
		Cached:    max64(curr.Cached-prev.Cached, 0),
		Output:    max64(curr.Output-prev.Output, 0),
		Reasoning: max64(curr.Reasoning-prev.Reasoning, 0),
		Total:     max64(curr.Total-prev.Total, 0),
	}
}

func extractModel(v any) string {
	m, ok := v.(map[string]any)
	if !ok {
		return ""
	}
	if s := getStringAny(m, "model", "model_name"); s != "" {
		return s
	}
	if info := getMap(m, "info"); info != nil {
		if s := getStringAny(info, "model", "model_name"); s != "" {
			return s
		}
		if md := getMap(info, "metadata"); md != nil {
			if s := getString(md, "model"); s != "" {
				return s
			}
			if out := getMap(md, "output"); out != nil {
				if s := getString(out, "model"); s != "" {
					return s
				}
			}
		}
	}
	if md := getMap(m, "metadata"); md != nil {
		if s := getString(md, "model"); s != "" {
			return s
		}
		if out := getMap(md, "output"); out != nil {
			if s := getString(out, "model"); s != "" {
				return s
			}
		}
	}
	if item := getMap(m, "item"); item != nil {
		if s := getStringAny(item, "model", "model_name"); s != "" {
			return s
		}
		if md := getMap(item, "metadata"); md != nil {
			if s := getString(md, "model"); s != "" {
				return s
			}
			if out := getMap(md, "output"); out != nil {
				if s := getString(out, "model"); s != "" {
					return s
				}
			}
		}
	}
	if payload := getMap(m, "payload"); payload != nil {
		if s := extractModel(payload); s != "" {
			return s
		}
	}
	return ""
}

func dedupeClaude(obj map[string]any) string {
	msg := getMap(obj, "message")
	mid := getString(msg, "id")
	rid := firstNonEmpty(getString(obj, "requestId"), getString(obj, "request_id"))
	if mid != "" && rid != "" {
		return "mid:" + mid + ":rid:" + rid
	}
	if rid != "" {
		return "rid:" + rid
	}
	return ""
}

func dateLocal(ts, tz string) string {
	if strings.TrimSpace(ts) == "" {
		return ""
	}
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		if tt, e := time.Parse("2006-01-02 15:04:05", ts); e == nil {
			t = tt
		} else if len(ts) >= 10 {
			return ts[:10]
		} else {
			return ts
		}
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		loc = time.Local
	}
	return t.In(loc).Format("2006-01-02")
}

func getMap(m map[string]any, key string) map[string]any {
	if m == nil {
		return nil
	}
	if v, ok := m[key]; ok {
		if mm, ok := v.(map[string]any); ok {
			return mm
		}
	}
	return nil
}

func getAny(m map[string]any, key string) any {
	if m == nil {
		return nil
	}
	return m[key]
}

func getString(m map[string]any, key string) string {
	if m == nil {
		return ""
	}
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func getStringAny(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if s := getString(m, k); s != "" {
			return s
		}
	}
	return ""
}

func getInt64Any(m map[string]any, keys ...string) int64 {
	for _, k := range keys {
		if m == nil {
			return 0
		}
		v, ok := m[k]
		if !ok || v == nil {
			continue
		}
		switch t := v.(type) {
		case float64:
			return int64(t)
		case int64:
			return t
		case int:
			return int64(t)
		case json.Number:
			i, _ := t.Int64()
			return i
		case string:
			var n json.Number = json.Number(strings.TrimSpace(t))
			i, err := n.Int64()
			if err == nil {
				return i
			}
		}
	}
	return 0
}

func getFloatAny(m map[string]any, keys ...string) float64 {
	for _, k := range keys {
		if m == nil {
			return 0
		}
		v, ok := m[k]
		if !ok || v == nil {
			continue
		}
		switch t := v.(type) {
		case float64:
			return t
		case int64:
			return float64(t)
		case int:
			return float64(t)
		case json.Number:
			f, _ := t.Float64()
			return f
		case string:
			var n json.Number = json.Number(strings.TrimSpace(t))
			f, err := n.Float64()
			if err == nil {
				return f
			}
		}
	}
	return 0
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		s = strings.TrimSpace(s)
		if s != "" {
			return s
		}
	}
	return ""
}

func getProjectFromCwd(cwd string) string {
	cwd = normalizePath(cwd)
	if cwd == "" {
		return ""
	}
	if idx := strings.LastIndex(cwd, "/"); idx >= 0 && idx+1 < len(cwd) {
		return cwd[idx+1:]
	}
	return cwd
}

func parentName(p string, up int) string {
	p = normalizePath(p)
	parts := strings.Split(p, "/")
	if len(parts) <= up {
		return ""
	}
	return parts[len(parts)-1-up]
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (p parsedEntry) ObjString(key string) string {
	return getString(p.Obj, key)
}
