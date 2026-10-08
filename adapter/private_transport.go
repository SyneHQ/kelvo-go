package adapter

// PrivateTransport identifies only an inherited channel. The parent chooses it
// after source authorization. It carries no route, proof, ticket or signing key.
type PrivateTransport struct {
	ControlFD int `json:"control_fd"`
}

func (r ProcessRequest) validatePrivateTransport() error {
	if r.PrivateTransport == nil {
		return nil
	}
	if r.PrivateTransport.ControlFD != 7 || r.SourceFile != nil || r.Runtime != nil || (r.Source.Engine != "postgresql" && r.Source.Engine != "mysql") || r.Request.Kind.Watcher() || r.Request.Kind.Ingestion() {
		return ErrInvalid
	}
	return nil
}
