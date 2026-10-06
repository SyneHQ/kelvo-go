package filesnapshot

import (
	"strings"
	"testing"
)

func TestDescriptorRejectsUnboundedOrExecutableFormats(t *testing.T) {
	valid := Descriptor{Version: Version, Format: "csv", Bytes: 10, SHA256: strings.Repeat("a", 64)}
	if valid.Validate() != nil {
		t.Fatal("valid descriptor rejected")
	}
	for _, edit := range []func(*Descriptor){
		func(d *Descriptor) { d.Version++ }, func(d *Descriptor) { d.Format = "https://source" }, func(d *Descriptor) { d.Format = "sql" },
		func(d *Descriptor) { d.Bytes = 0 }, func(d *Descriptor) { d.Bytes = MaxBytes + 1 }, func(d *Descriptor) { d.SHA256 = "invalid" },
	} {
		d := valid
		edit(&d)
		if d.Validate() == nil {
			t.Fatal("invalid descriptor accepted", d)
		}
	}
}
