package usage

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const liteLLMPricingURL = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json"

type pricingRecord struct {
	InputCostPerToken           float64 `json:"input_cost_per_token"`
	OutputCostPerToken          float64 `json:"output_cost_per_token"`
	CacheCreationInputTokenCost float64 `json:"cache_creation_input_token_cost"`
	CacheReadInputTokenCost     float64 `json:"cache_read_input_token_cost"`
	Provider                    string  `json:"litellm_provider"`
}

type pricingCatalog map[string]pricingRecord

func fetchPricingCatalog(ctx context.Context) (pricingCatalog, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, liteLLMPricingURL, nil)
	if err != nil {
		return nil, err
	}
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("pricing fetch failed: %s", resp.Status)
	}
	var out pricingCatalog
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func resolvePricing(c pricingCatalog, model string, tool Tool) (pricingRecord, bool) {
	if len(c) == 0 || strings.TrimSpace(model) == "" {
		return pricingRecord{}, false
	}
	candidates := modelCandidates(model, tool)
	for _, k := range candidates {
		if v, ok := c[k]; ok {
			return v, true
		}
	}
	return pricingRecord{}, false
}

func modelCandidates(model string, tool Tool) []string {
	model = strings.TrimSpace(model)
	if model == "" {
		return nil
	}
	set := map[string]bool{model: true}

	if tool == ToolCodex && model == "gpt-5-codex" {
		set["gpt-5"] = true
	}

	var prefixes []string
	switch tool {
	case ToolClaude:
		prefixes = []string{"anthropic/", "openrouter/anthropic/"}
	case ToolCodex:
		prefixes = []string{"openai/", "azure/", "openrouter/openai/"}
	}

	for _, p := range prefixes {
		set[p+model] = true
		if tool == ToolCodex && model == "gpt-5-codex" {
			set[p+"gpt-5"] = true
		}
	}

	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	return out
}

func calculateCost(tool Tool, rec pricingRecord, input, cachedInput, output, cacheCreate, cacheRead int64) float64 {
	inputRate := rec.InputCostPerToken
	outputRate := rec.OutputCostPerToken
	cacheCreateRate := rec.CacheCreationInputTokenCost
	cacheReadRate := rec.CacheReadInputTokenCost
	if cacheCreateRate == 0 {
		cacheCreateRate = inputRate
	}
	if cacheReadRate == 0 {
		cacheReadRate = inputRate
	}

	switch tool {
	case ToolCodex:
		if cachedInput > input {
			cachedInput = input
		}
		nonCached := input - cachedInput
		return float64(nonCached)*inputRate + float64(cachedInput)*cacheReadRate + float64(output)*outputRate
	default:
		return float64(input)*inputRate + float64(output)*outputRate + float64(cacheCreate)*cacheCreateRate + float64(cacheRead)*cacheReadRate
	}
}
