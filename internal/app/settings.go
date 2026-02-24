package app

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/derekurban/profilex-cli/internal/store"
)

const nativeProfileRef = "default"

func settingsPathsForTool(tool store.Tool) ([]string, error) {
	switch tool {
	case store.ToolCodex:
		return []string{"config.toml"}, nil
	case store.ToolClaude:
		return []string{"settings.json"}, nil
	default:
		return nil, fmt.Errorf("unsupported tool %q for settings presets", tool)
	}
}

func isNativeProfileAlias(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "default", "native", "@default", "@native":
		return true
	default:
		return false
	}
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func nativeConfigDirForTool(tool store.Tool) (string, error) {
	switch tool {
	case store.ToolCodex:
		if custom := strings.TrimSpace(os.Getenv("PROFILEX_NATIVE_CODEX_HOME")); custom != "" {
			return filepath.Clean(custom), nil
		}
	case store.ToolClaude:
		if custom := strings.TrimSpace(os.Getenv("PROFILEX_NATIVE_CLAUDE_CONFIG_DIR")); custom != "" {
			return filepath.Clean(custom), nil
		}
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	switch tool {
	case store.ToolCodex:
		return filepath.Join(home, ".codex"), nil
	case store.ToolClaude:
		primary := filepath.Join(home, ".claude")
		legacy := filepath.Join(home, ".config", "claude")
		if dirExists(primary) || !dirExists(legacy) {
			return primary, nil
		}
		return legacy, nil
	default:
		return "", fmt.Errorf("unsupported tool %q for native settings", tool)
	}
}

func nativeSessionDirForTool(tool store.Tool) (string, error) {
	configDir, err := nativeConfigDirForTool(tool)
	if err != nil {
		return "", err
	}
	leaf, err := sessionLeafForTool(tool)
	if err != nil {
		return "", err
	}
	return filepath.Join(configDir, leaf), nil
}

func (m *Manager) expectedPresetDir(tool store.Tool, preset string) (string, error) {
	if err := store.ValidatePresetName(preset); err != nil {
		return "", err
	}
	expected := filepath.Join(m.Root(), "presets", string(tool), preset)
	abs, err := filepath.Abs(expected)
	if err != nil {
		return "", err
	}
	return filepath.Clean(abs), nil
}

func (m *Manager) touchPreset(tool store.Tool, preset string, ts time.Time) error {
	return m.store.Update(func(st *store.State) error {
		if _, p := store.FindSettingsPreset(st, tool, preset); p != nil {
			p.UpdatedAt = ts
			return nil
		}
		st.SettingsPresets = append(st.SettingsPresets, store.SettingsPreset{
			Tool:      tool,
			Name:      preset,
			CreatedAt: ts,
			UpdatedAt: ts,
		})
		return nil
	})
}

func (m *Manager) resolveSettingsProfileDir(st *store.State, tool store.Tool, ref string) (string, string, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return "", "", fmt.Errorf("profile reference cannot be empty")
	}
	if _, p := store.FindProfile(st, tool, ref); p != nil {
		dir, err := m.validatedManagedProfileDir(*p)
		if err != nil {
			return "", "", err
		}
		return dir, p.Name, nil
	}
	if !isNativeProfileAlias(ref) {
		return "", "", fmt.Errorf("profile not found: %s/%s", tool, ref)
	}
	dir, err := nativeConfigDirForTool(tool)
	if err != nil {
		return "", "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", err
	}
	return filepath.Clean(dir), nativeProfileRef, nil
}

func (m *Manager) NativeConfigDir(tool store.Tool) (string, error) {
	return nativeConfigDirForTool(tool)
}

func (m *Manager) NativeSessionDir(tool store.Tool) (string, error) {
	return nativeSessionDirForTool(tool)
}

func (m *Manager) applyPresetDirToProfile(profileDir, presetDir string, tool store.Tool) error {
	paths, err := settingsPathsForTool(tool)
	if err != nil {
		return err
	}
	for _, rel := range paths {
		src := filepath.Join(presetDir, rel)
		dst := filepath.Join(profileDir, rel)
		if err := syncPath(src, dst); err != nil {
			return fmt.Errorf("sync settings path %q: %w", rel, err)
		}
	}
	return nil
}

func (m *Manager) applyPresetToDir(tool store.Tool, preset, profileDir string) error {
	presetDir, err := m.expectedPresetDir(tool, preset)
	if err != nil {
		return err
	}
	if _, err := os.Stat(presetDir); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("settings preset not found: %s/%s", tool, preset)
		}
		return err
	}
	return m.applyPresetDirToProfile(profileDir, presetDir, tool)
}

// SnapshotSettings stores tool-native settings from a source profile into a
// named preset.
func (m *Manager) SnapshotSettings(tool store.Tool, sourceProfile, preset string) (int, error) {
	if err := store.ValidatePresetName(preset); err != nil {
		return 0, err
	}
	st, err := m.Load()
	if err != nil {
		return 0, err
	}
	sourceDir, _, err := m.resolveSettingsProfileDir(st, tool, sourceProfile)
	if err != nil {
		return 0, err
	}
	presetDir, err := m.expectedPresetDir(tool, preset)
	if err != nil {
		return 0, err
	}
	if err := os.MkdirAll(presetDir, 0o755); err != nil {
		return 0, err
	}

	paths, err := settingsPathsForTool(tool)
	if err != nil {
		return 0, err
	}
	for _, rel := range paths {
		src := filepath.Join(sourceDir, rel)
		dst := filepath.Join(presetDir, rel)
		if err := syncPath(src, dst); err != nil {
			return 0, fmt.Errorf("snapshot settings path %q: %w", rel, err)
		}
	}

	now := time.Now().UTC()
	if err := m.touchPreset(tool, preset, now); err != nil {
		return 0, err
	}
	return 0, nil
}

func (m *Manager) ApplySettingsPreset(tool store.Tool, preset, profileName string) error {
	st, err := m.Load()
	if err != nil {
		return err
	}
	profileDir, _, err := m.resolveSettingsProfileDir(st, tool, profileName)
	if err != nil {
		return err
	}
	if err := m.applyPresetToDir(tool, preset, profileDir); err != nil {
		return err
	}
	return m.touchPreset(tool, preset, time.Now().UTC())
}

func (m *Manager) SetSettingsSync(tool store.Tool, profileName, preset string, enabled bool) error {
	st, err := m.Load()
	if err != nil {
		return err
	}
	_, canonicalProfile, err := m.resolveSettingsProfileDir(st, tool, profileName)
	if err != nil {
		return err
	}
	if enabled {
		if err := store.ValidatePresetName(preset); err != nil {
			return err
		}
		presetDir, err := m.expectedPresetDir(tool, preset)
		if err != nil {
			return err
		}
		if _, err := os.Stat(presetDir); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("settings preset not found: %s/%s", tool, preset)
			}
			return err
		}
		if err := m.ApplySettingsPreset(tool, preset, canonicalProfile); err != nil {
			return err
		}
	}

	return m.store.Update(func(st *store.State) error {
		idx, existing := store.FindSettingsSync(st, tool, canonicalProfile)
		if !enabled {
			if existing != nil {
				st.SettingsSync = append(st.SettingsSync[:idx], st.SettingsSync[idx+1:]...)
			}
			return nil
		}

		now := time.Now().UTC()
		if existing != nil {
			st.SettingsSync[idx].Preset = preset
			st.SettingsSync[idx].UpdatedAt = now
			return nil
		}
		st.SettingsSync = append(st.SettingsSync, store.SettingsSync{
			Tool:      tool,
			Profile:   canonicalProfile,
			Preset:    preset,
			UpdatedAt: now,
		})
		return nil
	})
}

func (m *Manager) ApplySyncedSettings(profile store.Profile) (bool, string, error) {
	return false, "", nil
}

func (m *Manager) ListSettings(tool *store.Tool) ([]store.SettingsPreset, []store.SettingsSync, error) {
	st, err := m.Load()
	if err != nil {
		return nil, nil, err
	}
	presets := make([]store.SettingsPreset, 0, len(st.SettingsPresets))
	syncs := make([]store.SettingsSync, 0, len(st.SettingsSync))
	for _, p := range st.SettingsPresets {
		if tool != nil && p.Tool != *tool {
			continue
		}
		presets = append(presets, p)
	}
	for _, s := range st.SettingsSync {
		if tool != nil && s.Tool != *tool {
			continue
		}
		syncs = append(syncs, s)
	}
	return presets, syncs, nil
}

func (m *Manager) RenameSettingsPreset(tool store.Tool, oldName, newName string) error {
	if err := store.ValidatePresetName(newName); err != nil {
		return err
	}
	if strings.EqualFold(strings.TrimSpace(oldName), strings.TrimSpace(newName)) {
		return nil
	}

	oldDir, err := m.expectedPresetDir(tool, oldName)
	if err != nil {
		return err
	}
	newDir, err := m.expectedPresetDir(tool, newName)
	if err != nil {
		return err
	}
	if _, err := os.Stat(oldDir); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("settings preset not found: %s/%s", tool, oldName)
		}
		return err
	}
	if _, err := os.Stat(newDir); err == nil {
		return fmt.Errorf("target preset already exists: %s/%s", tool, newName)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(newDir), 0o755); err != nil {
		return err
	}
	if err := os.Rename(oldDir, newDir); err != nil {
		return err
	}

	return m.store.Update(func(st *store.State) error {
		idx, p := store.FindSettingsPreset(st, tool, oldName)
		if p == nil {
			return fmt.Errorf("settings preset not found in state: %s/%s", tool, oldName)
		}
		st.SettingsPresets[idx].Name = newName
		st.SettingsPresets[idx].UpdatedAt = time.Now().UTC()
		for i := range st.SettingsSync {
			if st.SettingsSync[i].Tool == tool && st.SettingsSync[i].Preset == oldName {
				st.SettingsSync[i].Preset = newName
				st.SettingsSync[i].UpdatedAt = time.Now().UTC()
			}
		}
		return nil
	})
}

func (m *Manager) DeleteSettingsPreset(tool store.Tool, name string) error {
	dir, err := m.expectedPresetDir(tool, name)
	if err != nil {
		return err
	}
	if _, err := os.Stat(dir); err == nil {
		if rmErr := os.RemoveAll(dir); rmErr != nil {
			return rmErr
		}
	} else if !os.IsNotExist(err) {
		return err
	}

	return m.store.Update(func(st *store.State) error {
		if idx, p := store.FindSettingsPreset(st, tool, name); p != nil {
			st.SettingsPresets = append(st.SettingsPresets[:idx], st.SettingsPresets[idx+1:]...)
		}
		filtered := st.SettingsSync[:0]
		for _, s := range st.SettingsSync {
			if s.Tool == tool && s.Preset == name {
				continue
			}
			filtered = append(filtered, s)
		}
		st.SettingsSync = filtered
		return nil
	})
}

func syncPath(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		if os.IsNotExist(err) {
			if rmErr := os.RemoveAll(dst); rmErr != nil && !os.IsNotExist(rmErr) {
				return rmErr
			}
			return nil
		}
		return err
	}
	if info.IsDir() {
		return copyDirReplace(src, dst)
	}
	return copyFileReplace(src, dst, info.Mode())
}

func copyDirReplace(src, dst string) error {
	if err := os.RemoveAll(dst); err != nil {
		return err
	}
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if d.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlink not supported in settings snapshot: %s", path)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return copyFileReplace(path, target, info.Mode())
	})
}

func copyFileReplace(src, dst string, mode fs.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if mode == 0 {
		mode = 0o644
	}
	return os.Chmod(dst, mode.Perm())
}
