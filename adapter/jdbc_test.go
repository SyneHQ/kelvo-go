package adapter

import (
	"testing"
	"time"
)

func TestJDBCRuntimeCannotSelectPathsOrCrossDescriptorSlots(t *testing.T) {
	now := time.Now()
	fresh := func() ProcessRequest {
		r := processFixture(t, now)
		r.Source.Engine = "h2"
		r.Source.DSN = ""
		r.Source.URL = "tls://database.example:9092"
		r.Source.Username = "user"
		r.Source.Password = "secret"
		r.Runtime = &JDBCRuntime{Version: 1, JavaFD: 7, JARFDs: []int{8, 9}, HeapMB: 96, DirectMB: 32}
		return r
	}
	if err := fresh().ValidateAt(now); err != nil {
		t.Fatal(err)
	}
	for _, edit := range []func(*ProcessRequest){
		func(r *ProcessRequest) { r.Runtime.JavaFD = 6 },
		func(r *ProcessRequest) { r.Runtime.JARFDs = []int{8, 10} },
		func(r *ProcessRequest) { r.Runtime.JARFDs = nil },
		func(r *ProcessRequest) { r.Runtime.HeapMB = 0 },
		func(r *ProcessRequest) { r.Source.Engine = "postgresql" },
		func(r *ProcessRequest) { r.Source.URL = "tls://db:9092/db;INIT=RUNSCRIPT" },
		func(r *ProcessRequest) { r.Source.URL = "tls://user:password@db:9092" },
		func(r *ProcessRequest) { r.Source.URL = "http://db:9092" },
		func(r *ProcessRequest) { r.Source.URL = "tls://db:9092?" },
		func(r *ProcessRequest) { r.Source.DSN = "jdbc:h2:mem:db" },
	} {
		r := fresh()
		edit(&r)
		if r.ValidateAt(now) == nil {
			t.Fatal("unsafe runtime accepted")
		}
	}
}

func TestJDBCCapabilitiesRemainExplicit(t *testing.T) {
	for _, engine := range []string{"h2", "hive", "spark", "db2", "sap_hana", "sap_ase"} {
		if err := JDBCCapabilities(engine).Validate(); err != nil {
			t.Fatal(engine, err)
		}
	}
	if JDBCProfile("arbitrary-java-class") || JDBCProfile("redis") {
		t.Fatal("unknown profile accepted")
	}
}
