//go:build darwin && !cgo

package runtime

func readOSProcessIdentity(int) (osProcessIdentity, error) {
	return osProcessIdentity{}, errProcessIdentityUnavailable
}

func listOSProcessIdentities() ([]osProcessIdentity, error) {
	return nil, errProcessIdentityUnavailable
}
