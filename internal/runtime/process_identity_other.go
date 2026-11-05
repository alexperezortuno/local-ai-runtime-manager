//go:build !linux && !darwin

package runtime

func readOSProcessIdentity(int) (osProcessIdentity, error) {
	return osProcessIdentity{}, errProcessIdentityUnavailable
}

func listOSProcessIdentities() ([]osProcessIdentity, error) {
	return nil, errProcessIdentityUnavailable
}
