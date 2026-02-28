package usage

import "github.com/derekurban/profilex-cli/internal/store"

type CostMode string

const (
	CostModeAuto      CostMode = "auto"
	CostModeCalculate CostMode = "calculate"
	CostModeDisplay   CostMode = "display"
)

type Tool string

const (
	ToolClaude   Tool = "claude"
	ToolCodex    Tool = "codex"
	ToolOpenClaw Tool = "openclaw"
	ToolUnknown  Tool = "unknown"
)

type NormalizedEvent struct {
	ID                        string   `json:"id"`
	TimestampUTC              string   `json:"timestampUtc"`
	DateLocal                 string   `json:"dateLocal"`
	Tool                      Tool     `json:"tool"`
	ProfileID                 string   `json:"profileId"`
	ProfileName               string   `json:"profileName"`
	IsProfilexManaged         bool     `json:"isProfilexManaged"`
	SourceRoot                string   `json:"sourceRoot"`
	SourceFile                string   `json:"sourceFile"`
	SessionID                 string   `json:"sessionId"`
	Project                   string   `json:"project"`
	Model                     string   `json:"model"`
	IsFallbackModel           bool     `json:"isFallbackModel"`
	InputTokens               int64    `json:"inputTokens"`
	CachedInputTokens         int64    `json:"cachedInputTokens"`
	OutputTokens              int64    `json:"outputTokens"`
	ReasoningOutputTokens     int64    `json:"reasoningOutputTokens"`
	CacheCreationTokens       int64    `json:"cacheCreationTokens"`
	CacheReadTokens           int64    `json:"cacheReadTokens"`
	RawTotalTokens            int64    `json:"rawTotalTokens"`
	NormalizedTotalTokens     int64    `json:"normalizedTotalTokens"`
	ObservedCostUSD           float64  `json:"observedCostUSD"`
	CalculatedCostUSD         float64  `json:"calculatedCostUSD"`
	EffectiveCostUSD          float64  `json:"effectiveCostUSD"`
	CostModeUsed              CostMode `json:"costModeUsed"`
	IsSharedSession           bool     `json:"isSharedSession,omitempty"`
	SharedSessionProfileIDs   []string `json:"sharedSessionProfileIds,omitempty"`
	SharedSessionProfileNames []string `json:"sharedSessionProfileNames,omitempty"`
	SharedSessionSources      []string `json:"sharedSessionSources,omitempty"`
}

type UnifiedSourceSummary struct {
	ProfilexStatePath string   `json:"profilexStatePath"`
	UsageRoots        []string `json:"usageRoots"`
	UsageFiles        []string `json:"usageFiles"`
}

type UnifiedLocalBundle struct {
	SchemaVersion  int                  `json:"schemaVersion"`
	GeneratedAtUTC string               `json:"generatedAtUtc"`
	Timezone       string               `json:"timezone"`
	CostMode       CostMode             `json:"costMode"`
	PricingLoaded  bool                 `json:"pricingLoaded"`
	ProfilexState  *store.State         `json:"profilexState"`
	Events         []NormalizedEvent    `json:"events"`
	Source         UnifiedSourceSummary `json:"source"`
	Notes          []string             `json:"notes"`
}

type GenerateOptions struct {
	RootDir  string
	Deep     bool
	MaxFiles int
	Timezone string
	CostMode CostMode
}
