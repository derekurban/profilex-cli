package cli

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/derekurban/profilex-cli/internal/store"
)

func cmdSettings(rootDir string, args []string) error {
	if len(args) == 0 || hasHelp(args) {
		fmt.Printf("Usage:\n")
		fmt.Printf("  profilex settings snapshot <tool> <profile|default> <preset>\n")
		fmt.Printf("  profilex settings apply <tool> <preset> <profile|default>\n")
		fmt.Printf("  profilex settings list [--tool <tool>] [--json]\n")
		fmt.Printf("\n")
		fmt.Printf("Special profile aliases: default, native, @default, @native\n")
		return nil
	}

	sub := strings.ToLower(strings.TrimSpace(args[0]))
	rest := args[1:]
	switch sub {
	case "snapshot":
		return cmdSettingsSnapshot(rootDir, rest)
	case "apply":
		return cmdSettingsApply(rootDir, rest)
	case "list":
		return cmdSettingsList(rootDir, rest)
	default:
		return fmt.Errorf("unknown settings subcommand: %s", sub)
	}
}

func cmdSettingsSnapshot(rootDir string, args []string) error {
	if hasHelp(args) || len(args) != 3 {
		fmt.Printf("Usage: profilex settings snapshot <tool> <profile|default> <preset>\n")
		return nil
	}
	tool, err := parseTool(args[0])
	if err != nil {
		return err
	}
	mgr, err := newManager(rootDir)
	if err != nil {
		return err
	}
	_, err = mgr.SnapshotSettings(tool, args[1], args[2])
	if err != nil {
		return err
	}
	fmt.Printf("%s Snapshot saved: %s/%s from %s\n", Green("ok"), tool, args[2], args[1])
	fmt.Printf("   Included paths: %s\n", Dim(settingsPathHint(tool)))
	return nil
}

func cmdSettingsApply(rootDir string, args []string) error {
	if hasHelp(args) || len(args) != 3 {
		fmt.Printf("Usage: profilex settings apply <tool> <preset> <profile|default>\n")
		return nil
	}
	tool, err := parseTool(args[0])
	if err != nil {
		return err
	}
	mgr, err := newManager(rootDir)
	if err != nil {
		return err
	}
	if err := mgr.ApplySettingsPreset(tool, args[1], args[2]); err != nil {
		return err
	}
	fmt.Printf("%s Applied settings preset %s/%s to %s\n", Green("ok"), tool, args[1], args[2])
	return nil
}

func cmdSettingsList(rootDir string, args []string) error {
	toolFlag, args := extractFlag(args, "--tool")
	jsonOut, args := extractBool(args, "--json")
	if hasHelp(args) || len(args) > 0 {
		fmt.Printf("Usage: profilex settings list [--tool <tool>] [--json]\n")
		return nil
	}

	var filter *store.Tool
	if toolFlag != "" {
		t, err := parseTool(toolFlag)
		if err != nil {
			return err
		}
		filter = &t
	}

	mgr, err := newManager(rootDir)
	if err != nil {
		return err
	}
	presets, _, err := mgr.ListSettings(filter)
	if err != nil {
		return err
	}

	if jsonOut {
		payload := map[string]any{
			"presets": presets,
		}
		b, _ := json.MarshalIndent(payload, "", "  ")
		fmt.Println(string(b))
		return nil
	}

	fmt.Printf("%s\n", Bold("Settings Presets"))
	if len(presets) == 0 {
		fmt.Println("  (none)")
	} else {
		for _, p := range presets {
			fmt.Printf("  - %s/%s\n", p.Tool, p.Name)
		}
	}
	fmt.Println()
	fmt.Printf("%s\n", Bold("Native Defaults"))
	for _, t := range store.SupportedTools {
		if filter != nil && t != *filter {
			continue
		}
		cfg, cfgErr := mgr.NativeConfigDir(t)
		sess, sessErr := mgr.NativeSessionDir(t)
		if cfgErr != nil || sessErr != nil {
			fmt.Printf("  - %s/default (error resolving paths)\n", t)
			continue
		}
		fmt.Printf("  - %s/default\n", t)
		fmt.Printf("      config:  %s\n", Dim(cfg))
		fmt.Printf("      session: %s\n", Dim(sess))
	}
	return nil
}

func settingsPathHint(tool store.Tool) string {
	switch tool {
	case store.ToolCodex:
		return "config.toml"
	case store.ToolClaude:
		return "settings.json"
	default:
		return "(unknown)"
	}
}
