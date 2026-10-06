package operations

import "testing"

func TestObjectInspectionContractBindsKindAndTarget(t *testing.T) {
	request := Request{Version: Version, Kind: MetadataInspect, Connection: ConnectionRef{ID: "saved", Database: "PDB"}, Spec: Spec{Metadata: &MetadataSpec{Object: "objects", ObjectKind: "package", Target: ObjectRef{Catalog: "PDB", Name: "42"}, Limit: 500}}}
	if request.Validate() != nil {
		t.Fatal("valid object request rejected")
	}
	for _, change := range []func(*MetadataSpec){
		func(m *MetadataSpec) { m.ObjectKind = "execute" },
		func(m *MetadataSpec) { m.ObjectKind = "" },
		func(m *MetadataSpec) { m.Limit = 501 },
		func(m *MetadataSpec) { m.Cursor = "next" },
		func(m *MetadataSpec) { m.Object = "tables" },
	} {
		bad, err := Clone(request)
		if err != nil {
			t.Fatal(err)
		}
		change(bad.Spec.Metadata)
		if bad.Validate() == nil {
			t.Fatal("ambiguous object request accepted")
		}
	}
	before, _ := Digest(request)
	request.Spec.Metadata.ObjectKind = "package_body"
	after, _ := Digest(request)
	if before == after {
		t.Fatal("object kind not covered by request digest")
	}
}
