package runtime

import "local-ai-runtime/internal/corecontract"

// ProcessJournal is implemented by the existing Runtime Core journal. Keeping
// this narrow interface avoids a second process journal or a dependency on the
// inference registry's implementation details.
type ProcessJournal interface {
	PersistProcess(corecontract.ProcessInstance) error
	ProcessInstances() []corecontract.ProcessInstance
	OwnerInstanceID() string
}
