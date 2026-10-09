//go:build linux

package runtime

import (
	"context"
	"net"
	"os"
	"strconv"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/adapters/go/connectors/relational"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker/childipc"
	"github.com/SYNEHQ/kelvo-go/operations"
)

// Only the parent-provided channel can open a private source. The descriptor
// carries no issuer, proof, ticket, route or endpoint override.
func openPrivateSource(ctx context.Context, spec adapter.ConnectionSpec, request operations.Request, descriptor *adapter.PrivateTransport) (adapter.Session, func() error, error) {
	if descriptor == nil || descriptor.ControlFD != 7 || (spec.Engine != "postgresql" && spec.Engine != "mysql") || request.Kind.Watcher() || request.Kind.Ingestion() {
		return nil, nil, adapter.ErrUnsupported
	}
	connection, err := sourceConnection(spec)
	if err != nil {
		return nil, nil, err
	}
	authority := net.JoinHostPort(connection.Host, strconv.Itoa(connection.Port))
	client, err := childipc.NewClient(os.NewFile(7, "private-source-channel"), authority)
	if err != nil {
		return nil, nil, adapter.ErrInvalid
	}
	connection.DialContext = client.DialContext
	if connection.Engine == "postgresql" {
		connection.DialCancellation = client.DialCancellation
	}
	registry, err := New(relational.NewPostgreSQL(), relational.NewMySQL())
	if err != nil {
		client.Close()
		return nil, nil, err
	}
	session, err := registry.Open(ctx, connection, request)
	// The caller closes the database session before it closes the IPC channel.
	// The parent retains physical transport custody after this process exits.
	return session, client.Close, err
}
