package worker

import (
	"testing"

	"github.com/SYNEHQ/kelvo-go/internal/admission"
	"github.com/SYNEHQ/kelvo-go/internal/containment"
)

// Separate admission classes reserve slots, not CPU. Under contention a warm
// interactive read worker must outweigh a one-shot child, which must outweigh
// every background-class child.
func TestChildCPUWeightsFavourInteractiveReadWorkers(t *testing.T) {
	worker, oneShot := int64(containment.CPUWeightReadWorker), operationCPUWeight(admission.ClassInteractive)
	if !(worker > oneShot) {
		t.Fatalf("read worker %d must outweigh a one-shot child %d", worker, oneShot)
	}
	for _, class := range []admission.Class{admission.ClassOperation, admission.ClassRefresh, admission.ClassExport} {
		if background := operationCPUWeight(class); background < 1 || background >= oneShot {
			t.Fatalf("%s child weight %d must be positive and below the one-shot weight %d", class, background, oneShot)
		}
	}
}
