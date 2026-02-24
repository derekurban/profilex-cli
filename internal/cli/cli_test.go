package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/derekurban/profilex-cli/internal/app"
	"github.com/derekurban/profilex-cli/internal/store"
)

func captureRunOutput(t *testing.T, fn func() int) (stdout string, stderr string, code int) {
	t.Helper()

	oldStdout := os.Stdout
	oldStderr := os.Stderr
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = outW
	os.Stderr = errW

	code = fn()

	_ = outW.Close()
	_ = errW.Close()
	os.Stdout = oldStdout
	os.Stderr = oldStderr

	outB, _ := io.ReadAll(outR)
	errB, _ := io.ReadAll(errR)
	_ = outR.Close()
	_ = errR.Close()
	return string(outB), string(errB), code
}

func TestRunVersionUsesInjectedVersion(t *testing.T) {
	old := version
	version = "1.2.3"
	defer func() { version = old }()

	stdout, stderr, code := captureRunOutput(t, func() int { return Run([]string{"version"}) })
	if code != 0 {
		t.Fatalf("expected exit 0, got %d", code)
	}
	if stderr != "" {
		t.Fatalf("expected no stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, "profilex 1.2.3") {
		t.Fatalf("unexpected version output: %q", stdout)
	}
}

func TestResolvedVersionDefaultsToDev(t *testing.T) {
	old := version
	version = ""
	defer func() { version = old }()

	if got := resolvedVersion(); got != "dev" {
		t.Fatalf("expected dev fallback, got %q", got)
	}
}

func TestRunUnknownCommandReturnsError(t *testing.T) {
	_, stderr, code := captureRunOutput(t, func() int { return Run([]string{"does-not-exist"}) })
	if code != 1 {
		t.Fatalf("expected exit 1, got %d", code)
	}
	if !strings.Contains(stderr, "Unknown command: does-not-exist") {
		t.Fatalf("expected unknown command error, got %q", stderr)
	}
}

func TestRemoveOwnedBinaryRequiresMarker(t *testing.T) {
	dir := t.TempDir()
	binPath := filepath.Join(dir, "profilex")
	if err := os.WriteFile(binPath, []byte("bin"), 0o755); err != nil {
		t.Fatal(err)
	}

	removed, err := removeOwnedBinary(binPath)
	if err != nil {
		t.Fatal(err)
	}
	if removed {
		t.Fatalf("binary should not be removed without ownership marker")
	}
	if _, err := os.Stat(binPath); err != nil {
		t.Fatalf("binary should still exist: %v", err)
	}
}

func TestRemoveOwnedBinaryRemovesManagedBinaryAndMarker(t *testing.T) {
	dir := t.TempDir()
	binPath := filepath.Join(dir, "profilex")
	if err := os.WriteFile(binPath, []byte("bin"), 0o755); err != nil {
		t.Fatal(err)
	}

	markerPath := ownershipMarkerPath(binPath)
	content := fmt.Sprintf("%s\npath=%s\n", ownershipMarkerMagic, binPath)
	if err := os.WriteFile(markerPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	removed, err := removeOwnedBinary(binPath)
	if err != nil {
		t.Fatal(err)
	}
	if !removed {
		t.Fatalf("expected binary removal when marker exists")
	}
	if _, err := os.Stat(binPath); !os.IsNotExist(err) {
		t.Fatalf("binary should be removed")
	}
	if _, err := os.Stat(markerPath); !os.IsNotExist(err) {
		t.Fatalf("ownership marker should be removed")
	}
}

func TestIsInstallerManagedBinaryRejectsPathMismatch(t *testing.T) {
	dir := t.TempDir()
	binPath := filepath.Join(dir, "profilex")
	if err := os.WriteFile(binPath, []byte("bin"), 0o755); err != nil {
		t.Fatal(err)
	}

	markerPath := ownershipMarkerPath(binPath)
	content := fmt.Sprintf("%s\npath=%s\n", ownershipMarkerMagic, filepath.Join(dir, "other", "profilex"))
	if err := os.WriteFile(markerPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	if isInstallerManagedBinary(binPath) {
		t.Fatalf("expected marker path mismatch to be rejected")
	}
}

func TestCmdShimInstallReturnsErrorWhenAnyInstallFails(t *testing.T) {
	root := t.TempDir()
	mgr, err := app.NewManager(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := mgr.EnsureProfile(store.ToolClaude, "work"); err != nil {
		t.Fatal(err)
	}

	notDir := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(notDir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	err = cmdShimInstall(root, []string{"--dir", notDir})
	if err == nil {
		t.Fatalf("expected shim install to fail when target dir is invalid")
	}
	if !strings.Contains(err.Error(), "failed to install 1 shim(s)") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestShimEnvOutputsProfileEnvironment(t *testing.T) {
	root := t.TempDir()
	mgr, err := app.NewManager(root)
	if err != nil {
		t.Fatal(err)
	}

	profile, _, err := mgr.EnsureProfile(store.ToolClaude, "work")
	if err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := captureRunOutput(t, func() int {
		return Run([]string{"--root", root, "shim", "env", "claude", "work"})
	})
	if code != 0 {
		t.Fatalf("expected exit 0, got %d (stderr: %q)", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("expected no stderr, got %q", stderr)
	}
	if !strings.Contains(stdout, "CLAUDE_CONFIG_DIR="+profile.Dir) {
		t.Fatalf("missing CLAUDE_CONFIG_DIR output: %q", stdout)
	}
	if !strings.Contains(stdout, "PROFILEX_SHIM_NAME=claude-work") {
		t.Fatalf("missing PROFILEX_SHIM_NAME output: %q", stdout)
	}
}
