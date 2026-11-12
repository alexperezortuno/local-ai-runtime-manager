package adminauth

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

type publicKeyFile struct {
	Version int               `json:"version"`
	Keys    map[string]string `json:"keys"`
}

type privateKeyFile struct {
	Version int    `json:"version"`
	KeyID   string `json:"key_id"`
	Private string `json:"private_key"`
}

func loadPublicKeys(path string) (map[string]ed25519.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read admin public keyring: %w", err)
	}
	lstat, err := os.Lstat(path)
	if err != nil || lstat.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("admin public keyring must not be a symlink")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("admin public keyring must be private")
	}
	if uid, ok := fileOwner(info); ok && uid != os.Geteuid() {
		return nil, errors.New("admin public keyring is not owned by the LARM process user")
	}
	var file publicKeyFile
	if err := json.Unmarshal(data, &file); err != nil || file.Version != 1 || len(file.Keys) == 0 {
		return nil, errors.New("admin public keyring is corrupt or unsupported")
	}
	keys := make(map[string]ed25519.PublicKey, len(file.Keys))
	for id, encoded := range file.Keys {
		key, err := base64.RawStdEncoding.DecodeString(encoded)
		if err != nil || len(key) != ed25519.PublicKeySize || id == "" {
			return nil, errors.New("admin public keyring contains an invalid key")
		}
		keys[id] = ed25519.PublicKey(key)
	}
	return keys, nil
}

func loadPrivateKey(path string) (ed25519.PrivateKey, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("read admin private key: %w", err)
	}
	lstat, err := os.Lstat(path)
	if err != nil || lstat.Mode()&os.ModeSymlink != 0 {
		return nil, "", errors.New("admin private key must not be a symlink")
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&0077 != 0 {
		return nil, "", errors.New("admin private key must be private")
	}
	var file privateKeyFile
	if err := json.Unmarshal(data, &file); err != nil || file.Version != 1 || file.KeyID == "" {
		return nil, "", errors.New("admin private key is corrupt or unsupported")
	}
	key, err := base64.RawStdEncoding.DecodeString(file.Private)
	if err != nil || len(key) != ed25519.PrivateKeySize {
		return nil, "", errors.New("admin private key has an invalid size")
	}
	return ed25519.PrivateKey(key), file.KeyID, nil
}
