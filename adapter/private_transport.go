package adapter

// MaxPrivateDataOpenTimeoutMS bounds resolver authorization plus transport setup.
const MaxPrivateDataOpenTimeoutMS int64 = 32_000

// PrivateTransport identifies an inherited channel and its bounded data-open wait.
// The parent chooses it after authorization. It carries no route, proof, ticket or key.
type PrivateTransport struct {
	// DataOpenTimeoutMS is set only by the trusted parent. Zero keeps the two-second default.
	DataOpenTimeoutMS int64 `json:"data_open_timeout_ms,omitempty"`
	ControlFD         int   `json:"control_fd"`
	PostgresCleanup   bool  `json:"postgres_cleanup,omitempty"`
}

func (r ProcessRequest) validatePrivateTransport() error {
	if r.PrivateTransport == nil {
		return nil
	}
	if r.PrivateTransport.DataOpenTimeoutMS < 0 || r.PrivateTransport.DataOpenTimeoutMS > MaxPrivateDataOpenTimeoutMS {
		return ErrInvalid
	}
	if r.PrivateTransport.ControlFD != 7 || r.SourceFile != nil || r.Runtime != nil || (r.Source.Engine != "postgresql" && r.Source.Engine != "mysql") || r.Request.Kind.Watcher() || r.Request.Kind.Ingestion() {
		return ErrInvalid
	}
	if r.PrivateTransport.PostgresCleanup && (r.Source.Engine != "postgresql" || r.Request.Kind.Mutating()) {
		return ErrInvalid
	}
	return nil
}
