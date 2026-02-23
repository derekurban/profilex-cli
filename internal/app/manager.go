package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/derekurban/profilex-cli/internal/adapters"
	"github.com/derekurban/profilex-cli/internal/store"
)

type ExitCodeError struct {
	Code int
}

func (e ExitCodeError) Error() string {
	return fmt.Sprintf("process exited with code %d", e.Code)
}

type StatusRow struct {
	Profile store.Profile   `json:"profile"`
	Status  adapters.Status `json:"status"`
	Error   string          `json:"error,omitempty"`
}

type Manager struct {
	store *store.Store
}

func NewManager(root string) (*Manager, error) {
	s, err := store.New(root)
	if err != nil {
		return nil, err
	}
	return &Manager{store: s}, nil
}

func NewDefaultManager() (*Manager, error) {
	root, err := store.DefaultRoot()
	if err != nil {
		return nil, err
	}
	return NewManager(root)
}

func (m *Manager) Root() string {
	return m.store.Root()
}

func (m *Manager) Load() (*store.State, error) {
	return m.store.Load()
}

func (m *Manager) Save(st *store.State) error {
	return m.store.Save(st)
}

func (m *Manager) EnsureProfile(tool store.Tool, name string) (store.Profile, bool, error) {
	if err := store.ValidateProfileName(name); err != nil {
		return store.Profile{}, false, err
	}
	var (
		outProfile store.Profile
		created    bool
	)

	err := m.store.Update(func(st *store.State) error {
		if _, existing := store.FindProfile(st, tool, name); existing != nil {
			dir, err := m.validatedManagedProfileDir(*existing)
			if err != nil {
				return err
			}
			outProfile = *existing
			outProfile.Dir = dir
			return nil
		}

		dir, err := m.expectedProfileDir(tool, name)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}

		outProfile = store.Profile{
			Tool:      tool,
			Name:      name,
			Dir:       dir,
			CreatedAt: time.Now().UTC(),
		}
		st.Profiles = append(st.Profiles, outProfile)
		if _, ok := st.Defaults[tool]; !ok {
			st.Defaults[tool] = name
		}
		created = true
		return nil
	})
	if err != nil {
		return store.Profile{}, false, err
	}
	return outProfile, created, nil
}

// EnableSharedSessions wires the profile's session/history subdirectory to a
// shared directory under <root>/shared/<tool>/<leaf>.
//
// Claude uses "projects" as its session/history folder and Codex uses
// "sessions".
func (m *Manager) EnableSharedSessions(profile store.Profile) (string, error) {
	profileDir, err := m.validatedManagedProfileDir(profile)
	if err != nil {
		return "", err
	}

	leaf, err := sessionLeafForTool(profile.Tool)
	if err != nil {
		return "", err
	}

	sharedDir := filepath.Join(m.Root(), "shared", string(profile.Tool), leaf)
	if err := os.MkdirAll(sharedDir, 0o755); err != nil {
		return "", err
	}

	mountPath := filepath.Join(profileDir, leaf)
	if info, err := os.Lstat(mountPath); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(mountPath)
			if err != nil {
				return "", err
			}
			if !filepath.IsAbs(target) {
				target = filepath.Join(filepath.Dir(mountPath), target)
			}
			target = filepath.Clean(target)
			if samePath(target, filepath.Clean(sharedDir)) {
				return filepath.Clean(sharedDir), nil
			}
			return "", fmt.Errorf("%s already points to %q (expected %q)", mountPath, target, sharedDir)
		}

		if !info.IsDir() {
			return "", fmt.Errorf("%s exists and is not a directory", mountPath)
		}

		entries, err := os.ReadDir(mountPath)
		if err != nil {
			return "", err
		}
		if len(entries) > 0 {
			return "", fmt.Errorf("%s already contains data; refusing to replace with shared link", mountPath)
		}
		if err := os.Remove(mountPath); err != nil {
			return "", err
		}
	} else if !os.IsNotExist(err) {
		return "", err
	}

	if err := createDirLink(sharedDir, mountPath); err != nil {
		return "", err
	}

	return filepath.Clean(sharedDir), nil
}

func (m *Manager) GetProfile(st *store.State, tool store.Tool, name string) (store.Profile, error) {
	_, p := store.FindProfile(st, tool, name)
	if p == nil {
		return store.Profile{}, fmt.Errorf("profile not found: %s/%s", tool, name)
	}
	dir, err := m.validatedManagedProfileDir(*p)
	if err != nil {
		return store.Profile{}, err
	}
	out := *p
	out.Dir = dir
	return out, nil
}

func (m *Manager) ResolveProfile(st *store.State, tool store.Tool, profileOptional string) (store.Profile, error) {
	if profileOptional != "" {
		return m.GetProfile(st, tool, profileOptional)
	}
	name, ok := store.DefaultProfile(st, tool)
	if !ok {
		return store.Profile{}, fmt.Errorf("no default profile set for %s", tool)
	}
	return m.GetProfile(st, tool, name)
}

func (m *Manager) SetDefault(tool store.Tool, name string) error {
	return m.store.Update(func(st *store.State) error {
		if _, p := store.FindProfile(st, tool, name); p == nil {
			return fmt.Errorf("profile not found: %s/%s", tool, name)
		}
		st.Defaults[tool] = name
		return nil
	})
}

func (m *Manager) RenameProfile(tool store.Tool, oldName, newName string) error {
	if err := store.ValidateProfileName(newName); err != nil {
		return err
	}
	return m.store.Update(func(st *store.State) error {
		idx, p := store.FindProfile(st, tool, oldName)
		if p == nil {
			return fmt.Errorf("profile not found: %s/%s", tool, oldName)
		}
		if _, exists := store.FindProfile(st, tool, newName); exists != nil {
			return fmt.Errorf("target profile already exists: %s/%s", tool, newName)
		}

		oldDir, err := m.validatedManagedProfileDir(*p)
		if err != nil {
			return err
		}
		newDir, err := m.expectedProfileDir(tool, newName)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(newDir), 0o755); err != nil {
			return err
		}
		if err := os.Rename(oldDir, newDir); err != nil {
			return err
		}

		st.Profiles[idx].Name = newName
		st.Profiles[idx].Dir = newDir
		if st.Defaults[tool] == oldName {
			st.Defaults[tool] = newName
		}
		return nil
	})
}

func (m *Manager) RemoveProfile(tool store.Tool, name string, purge bool) error {
	return m.store.Update(func(st *store.State) error {
		idx, p := store.FindProfile(st, tool, name)
		if p == nil {
			return fmt.Errorf("profile not found: %s/%s", tool, name)
		}
		dir, err := m.validatedManagedProfileDir(*p)
		if err != nil {
			return err
		}
		if purge {
			if err := os.RemoveAll(dir); err != nil {
				return err
			}
		}

		st.Profiles = append(st.Profiles[:idx], st.Profiles[idx+1:]...)
		if st.Defaults[tool] == name {
			delete(st.Defaults, tool)
			for _, prof := range st.Profiles {
				if prof.Tool == tool {
					st.Defaults[tool] = prof.Name
					break
				}
			}
		}
		return nil
	})
}

func (m *Manager) RunTool(ctx context.Context, profile store.Profile, args []string) error {
	adapter, err := adapters.Get(profile.Tool)
	if err != nil {
		return err
	}
	cmd := adapter.RunCommand(profile.Dir, args)
	return runInteractive(ctx, cmd)
}

func (m *Manager) StatusForProfile(ctx context.Context, profile store.Profile) (adapters.Status, error) {
	adapter, err := adapters.Get(profile.Tool)
	if err != nil {
		return adapters.Status{}, err
	}
	return adapter.Status(ctx, profile.Dir)
}

func (m *Manager) StatusRows(ctx context.Context, filterTool *store.Tool) ([]StatusRow, error) {
	st, err := m.Load()
	if err != nil {
		return nil, err
	}
	rows := []StatusRow{}
	for _, p := range st.Profiles {
		if filterTool != nil && p.Tool != *filterTool {
			continue
		}
		dir, err := m.validatedManagedProfileDir(p)
		if err != nil {
			rows = append(rows, StatusRow{Profile: p, Error: err.Error()})
			continue
		}
		p.Dir = dir

		ctxOne, cancel := context.WithTimeout(ctx, 8*time.Second)
		status, sErr := m.StatusForProfile(ctxOne, p)
		cancel()
		row := StatusRow{Profile: p, Status: status}
		if sErr != nil {
			row.Error = sErr.Error()
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (m *Manager) expectedProfileDir(tool store.Tool, name string) (string, error) {
	expected := store.ProfileDir(m.Root(), tool, name)
	abs, err := filepath.Abs(expected)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

func (m *Manager) validatedManagedProfileDir(profile store.Profile) (string, error) {
	expected, err := m.expectedProfileDir(profile.Tool, profile.Name)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(profile.Dir) == "" {
		return "", fmt.Errorf("profile %s/%s has no directory", profile.Tool, profile.Name)
	}
	actual, err := filepath.Abs(profile.Dir)
	if err != nil {
		return "", err
	}
	actual = filepath.Clean(actual)
	if !samePath(actual, expected) {
		return "", fmt.Errorf(
			"profile %s/%s has unsafe directory %q (expected %q)",
			profile.Tool,
			profile.Name,
			profile.Dir,
			expected,
		)
	}
	return expected, nil
}

func samePath(a, b string) bool {
	if runtime.GOOS == "windows" {
		return strings.EqualFold(a, b)
	}
	return a == b
}

func sessionLeafForTool(tool store.Tool) (string, error) {
	switch tool {
	case store.ToolClaude:
		return "projects", nil
	case store.ToolCodex:
		return "sessions", nil
	default:
		return "", fmt.Errorf("unsupported tool %q for shared sessions", tool)
	}
}

func createDirLink(target, linkPath string) error {
	if err := os.MkdirAll(filepath.Dir(linkPath), 0o755); err != nil {
		return err
	}
	if err := os.Symlink(target, linkPath); err == nil {
		return nil
	} else if runtime.GOOS != "windows" {
		return err
	}

	cmd := exec.Command("cmd", "/C", "mklink", "/J", linkPath, target)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mklink failed: %v (%s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func runInteractive(ctx context.Context, cmd *exec.Cmd) error {
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-ctx.Done():
		_ = cmd.Process.Kill()
		return ctx.Err()
	case err := <-done:
		if err == nil {
			return nil
		}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ExitCodeError{Code: ee.ExitCode()}
		}
		return err
	}
}
