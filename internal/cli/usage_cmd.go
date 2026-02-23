package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/derekurban/profilex-cli/internal/usage"
)

func cmdUsage(rootDir string, args []string) error {
	if len(args) == 0 || hasHelp(args) {
		printUsageHelp()
		return nil
	}

	sub := args[0]
	rest := args[1:]
	switch sub {
	case "export":
		return cmdUsageExport(rootDir, rest)
	default:
		return fmt.Errorf("unknown usage subcommand %q", sub)
	}
}

func printUsageHelp() {
	fmt.Printf("Usage: profilex usage export [options]\n\n")
	fmt.Printf("Export unified local usage JSON bundle for ProfileX-UI.\n\n")
	fmt.Printf("Options:\n")
	fmt.Printf("  --out <file>           Output file path (default: ./public/local-unified-usage.json)\n")
	fmt.Printf("  --deep                 Also scan broader home directory for likely usage .jsonl files\n")
	fmt.Printf("  --max-files <n>        Max JSONL files to parse (default: 5000)\n")
	fmt.Printf("  --timezone <tz>        Timezone for daily rollups (default: local timezone)\n")
	fmt.Printf("  --cost-mode <mode>     auto|calculate|display (default: auto)\n")
}

func cmdUsageExport(rootDir string, args []string) error {
	outPath, args := extractFlag(args, "--out")
	deep, args := extractBool(args, "--deep")
	tz, args := extractFlag(args, "--timezone")
	costModeRaw, args := extractFlag(args, "--cost-mode")
	maxFilesRaw, args := extractFlag(args, "--max-files")

	if hasHelp(args) {
		printUsageHelp()
		return nil
	}
	if len(args) > 0 {
		return fmt.Errorf("unknown argument(s): %s", strings.Join(args, " "))
	}

	if strings.TrimSpace(outPath) == "" {
		outPath = filepath.Join("public", "local-unified-usage.json")
	}

	maxFiles := 5000
	if strings.TrimSpace(maxFilesRaw) != "" {
		n, err := strconv.Atoi(maxFilesRaw)
		if err != nil || n <= 0 {
			return fmt.Errorf("invalid --max-files value: %q", maxFilesRaw)
		}
		maxFiles = n
	}

	costMode := usage.CostModeAuto
	switch strings.ToLower(strings.TrimSpace(costModeRaw)) {
	case "", "auto":
		costMode = usage.CostModeAuto
	case "calculate":
		costMode = usage.CostModeCalculate
	case "display":
		costMode = usage.CostModeDisplay
	default:
		return fmt.Errorf("invalid --cost-mode %q (expected auto|calculate|display)", costModeRaw)
	}

	resolvedRoot, err := resolveRootDir(rootDir)
	if err != nil {
		return err
	}

	mgr, err := newManager(rootDir)
	if err != nil {
		return err
	}
	state, err := mgr.Load()
	if err != nil {
		return err
	}
	statePath := filepath.Join(resolvedRoot, "state.json")

	if strings.TrimSpace(tz) == "" {
		tz = time.Now().Location().String()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	bundle, err := usage.GenerateBundle(ctx, state, statePath, usage.GenerateOptions{
		RootDir:  resolvedRoot,
		Deep:     deep,
		MaxFiles: maxFiles,
		Timezone: tz,
		CostMode: costMode,
	})
	if err != nil {
		return err
	}

	if err := usage.WriteBundle(outPath, bundle); err != nil {
		return err
	}

	fmt.Printf("%s Wrote unified usage bundle\n", Green("✓"))
	fmt.Printf("   📄 File: %s\n", Dim(outPath))
	fmt.Printf("   🧾 Events: %d\n", len(bundle.Events))
	fmt.Printf("   📁 Roots: %d\n", len(bundle.Source.UsageRoots))
	fmt.Printf("   🗂️ Files: %d\n", len(bundle.Source.UsageFiles))
	if len(bundle.Notes) > 0 {
		fmt.Printf("   ℹ️ Notes: %d (inspect JSON notes array for details)\n", len(bundle.Notes))
	}

	return nil
}
