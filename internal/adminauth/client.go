package adminauth

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"local-ai-runtime/internal/recovery"
)

func Status(socket string) (string, error) {
	result, err := call(socket, request{Operation: "status"})
	if err != nil {
		return "", err
	}
	return result.Identity, nil
}

func Issue(socket, privateKeyPath, outputPath, incidentID, planID, operation string, ttl time.Duration) (map[string]any, error) {
	result, err := call(socket, request{Operation: "issue_prepare", IncidentID: incidentID, PlanID: planID, AllowedOperation: operation, TTLSeconds: int(ttl / time.Second)})
	if err != nil {
		return nil, err
	}
	if result.Payload == nil {
		return nil, errors.New("admin server returned no capability payload")
	}
	private, keyID, err := loadPrivateKey(privateKeyPath)
	if err != nil {
		return nil, err
	}
	capability, err := result.Payload.Sign(private, keyID)
	if err != nil {
		return nil, err
	}
	result, err = call(socket, request{Operation: "issue_commit", Capability: &capability})
	if err != nil {
		return nil, err
	}
	if err := writeCapability(outputPath, capability); err != nil {
		return nil, err
	}
	return result.Capability, nil
}

func IssueModelStop(socket, privateKeyPath, outputPath, modelID string, ttl time.Duration) (map[string]any, error) {
	result, err := call(socket, request{Operation: "issue_prepare", AllowedOperation: recovery.ModelStopAction, ModelID: modelID, TTLSeconds: int(ttl / time.Second)})
	if err != nil {
		return nil, err
	}
	if result.Payload == nil {
		return nil, errors.New("admin server returned no capability payload")
	}
	private, keyID, err := loadPrivateKey(privateKeyPath)
	if err != nil {
		return nil, err
	}
	capability, err := result.Payload.Sign(private, keyID)
	if err != nil {
		return nil, err
	}
	result, err = call(socket, request{Operation: "issue_commit", Capability: &capability})
	if err != nil {
		return nil, err
	}
	if err := writeCapability(outputPath, capability); err != nil {
		return nil, err
	}
	return result.Capability, nil
}

func Inspect(socket, path string) (map[string]any, error) {
	capability, err := readCapability(path)
	if err != nil {
		return nil, err
	}
	result, err := call(socket, request{Operation: "inspect", Capability: &capability})
	if err != nil {
		return nil, err
	}
	return result.Capability, nil
}

func Execute(socket, capabilityPath string) (map[string]any, error) {
	capability, err := readCapability(capabilityPath)
	if err != nil {
		return nil, err
	}
	result, err := call(socket, request{Operation: "reconcile", Capability: &capability})
	if err != nil {
		return nil, err
	}
	return result.Capability, nil
}

func StopModel(socket, capabilityPath, modelID string) (map[string]any, error) {
	capability, err := readCapability(capabilityPath)
	if err != nil {
		return nil, err
	}
	if capability.AllowedOperation != recovery.ModelStopAction || capability.ModelID != modelID {
		return nil, errors.New("capability is not scoped to this model stop")
	}
	result, err := call(socket, request{Operation: "model_stop", ModelID: modelID, Capability: &capability})
	if err != nil {
		return nil, err
	}
	return result.Capability, nil
}

func call(socket string, req request) (response, error) {
	conn, err := net.DialTimeout("unix", socket, 5*time.Second)
	if err != nil {
		return response{}, fmt.Errorf("admin socket unavailable: %w", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	data, err := json.Marshal(req)
	if err != nil {
		return response{}, err
	}
	if _, err := conn.Write(append(data, '\n')); err != nil {
		return response{}, err
	}
	var result response
	if err := json.NewDecoder(io.LimitReader(bufio.NewReader(conn), 1<<20)).Decode(&result); err != nil {
		return response{}, err
	}
	if !result.OK {
		if result.Error != "" {
			return result, errors.New(result.Error)
		}
		return result, fmt.Errorf("admin authorization rejected: %s", result.Reason)
	}
	return result, nil
}

func readCapability(path string) (recovery.Capability, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return recovery.Capability{}, err
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&0077 != 0 {
		return recovery.Capability{}, errors.New("capability file must be private")
	}
	var capability recovery.Capability
	if err := json.Unmarshal(data, &capability); err != nil {
		return recovery.Capability{}, fmt.Errorf("invalid capability file: %w", err)
	}
	return capability, nil
}

func writeCapability(path string, capability recovery.Capability) error {
	if path == "" {
		return errors.New("capability output path is required")
	}
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return errors.New("capability output must not be a symlink")
		}
		return errors.New("capability output already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	data, err := json.MarshalIndent(capability, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".larm-capability-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}
