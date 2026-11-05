//go:build larmtest

package recovery

import (
	"os"
	"time"
)

// SetCrashBoundaryHook is compiled only into the isolated crash fixture. It
// pauses after the requested durable prefix; the external controller owns the
// decision to send SIGKILL.
func SetCrashBoundaryHook(service *Service, phase ReconciliationPhase, marker, release string) {
	service.fault = func(actual ReconciliationPhase) error {
		if actual != phase {
			return nil
		}
		_ = os.WriteFile(marker, []byte(actual+"\n"), 0600)
		for {
			if _, err := os.Stat(release); err == nil {
				return nil
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}
