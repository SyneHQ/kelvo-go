// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package admission

// Class is selected by trusted execution code, never by an untrusted query.
// The zero value retains the legacy Request.Background mapping.
type Class string

const (
	ClassInteractive Class = "interactive"
	ClassExport      Class = "export"
	ClassRefresh     Class = "refresh"
)

const (
	interactiveIndex = iota
	exportIndex
	refreshIndex
	classCount
)

// ClassLimits optionally narrows a class within the shared global pool.
// All three ceilings are explicit: slots and memory must be positive; zero
// scratch permits only requests that need no scratch. These are ceilings, not
// additional capacity or guaranteed shares.
type ClassLimits struct {
	MaxConcurrent int
	MemoryBytes   int64
	ScratchBytes  int64
}

// ClassSnapshot is detached accounting for one fixed, trusted workload class.
// Used contains byte totals only, not a request's Class or Background selector.
type ClassSnapshot struct {
	Enabled bool
	Limits  ClassLimits
	Used    Request
	Active  int
	Waiting int
}

func classIndex(class Class) (int, error) {
	switch class {
	case ClassInteractive:
		return interactiveIndex, nil
	case ClassExport:
		return exportIndex, nil
	case ClassRefresh:
		return refreshIndex, nil
	default:
		return 0, ErrInvalid
	}
}

func requestClass(r Request) (int, error) {
	if r.Class == "" {
		if r.Background {
			return refreshIndex, nil
		}
		return interactiveIndex, nil
	}
	// Background=true historically declares refresh work. False is the Go
	// zero value and is not a second, contradictory interactive declaration.
	if r.Background && r.Class != ClassRefresh {
		return 0, ErrInvalid
	}
	return classIndex(r.Class)
}

func (c ClassLimits) validWithin(global Limits) bool {
	return c.MaxConcurrent > 0 && c.MaxConcurrent <= global.MaxConcurrent &&
		c.MemoryBytes > 0 && c.MemoryBytes <= global.MemoryBytes &&
		c.ScratchBytes >= 0 && c.ScratchBytes <= global.ScratchBytes
}

func cloneClassLimits(in map[Class]ClassLimits) map[Class]ClassLimits {
	if in == nil {
		return nil
	}
	out := make(map[Class]ClassLimits, len(in))
	for class, limits := range in {
		out[class] = limits
	}
	return out
}
