// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	mysqldriver "github.com/go-sql-driver/mysql"
	"net"
	"net/url"
	"os"
	"strconv"
	"sync"

	"github.com/SYNEHQ/kelvo-go/adapter"
	"github.com/SYNEHQ/kelvo-go/internal/transportbroker"
)

type privateChildServer interface {
	Serve(int) error
	Close(context.Context) error
}

// The channel is parent memory. The child receives only file, never this value.
// Only the verified private resolver can construct it. The ordinary resolver
// callback cannot select a channel or a descriptor.
type privateOperationChannel struct {
	inputSHA256 [32]byte
	server      privateChildServer
	file        *os.File
	session     *transportbroker.Session
	binding     transportbroker.Binding
	release     func()
	releaseOnce sync.Once
	closeFile   sync.Once
	fileError   error
}

func privateInputDigest(input adapter.ProcessRequest) ([32]byte, error) {
	if input.PrivateTransport != nil || input.Runtime != nil || input.SourceFile != nil || (input.Source.Engine != "postgresql" && input.Source.Engine != "mysql") {
		return [32]byte{}, adapter.ErrInvalid
	}
	raw, err := adapter.EncodeProcessRequest(input)
	defer clear(raw)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(raw), nil
}
func (c *privateOperationChannel) matches(input adapter.ProcessRequest) bool {
	sum, err := privateInputDigest(input)
	return c != nil && c.server != nil && c.file != nil && c.session != nil && c.session.BoundTo(c.binding) && err == nil && sum == c.inputSHA256
}
func (c *privateOperationChannel) closeChild() {
	c.closeFile.Do(func() {
		if c.file != nil {
			c.fileError = c.file.Close()
		}
	})
}
func (c *privateOperationChannel) start(pid int) { go c.server.Serve(pid) }
func (c *privateOperationChannel) close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	if c.server == nil || c.session == nil {
		return adapter.ErrInvalid
	}
	c.closeChild()
	channelErr := c.server.Close(ctx)
	sessionErr := c.session.Close(ctx)
	if err := errors.Join(c.fileError, channelErr, sessionErr); err != nil {
		return err
	}
	if c.release != nil {
		c.releaseOnce.Do(c.release)
	}
	return nil
}

func privateSourceAuthority(source adapter.ConnectionSpec) (string, error) {
	var host, port string
	switch source.Engine {
	case "postgresql":
		u, err := url.Parse(source.DSN)
		if err != nil {
			return "", adapter.ErrInvalid
		}
		host, port = u.Hostname(), u.Port()
		if port == "" {
			port = "5432"
		}
	case "mysql":
		config, err := mysqldriver.ParseDSN(source.DSN)
		if err != nil || config.Net != "tcp" {
			return "", adapter.ErrInvalid
		}
		host, port, err = net.SplitHostPort(config.Addr)
		if err != nil {
			return "", adapter.ErrInvalid
		}
	default:
		return "", adapter.ErrUnsupported
	}
	number, err := strconv.Atoi(port)
	if err != nil {
		return "", adapter.ErrInvalid
	}
	authority := net.JoinHostPort(host, strconv.Itoa(number))
	if transportbroker.ValidateAuthority(authority) != nil {
		return "", adapter.ErrInvalid
	}
	return authority, nil
}
func privateInputScope(input adapter.ProcessRequest, binding transportbroker.Binding) bool {
	authority, err := privateSourceAuthority(input.Source)
	identity, _ := json.Marshal([]string{binding.ClusterTenant, binding.Tenant})
	tenant := sha256.Sum256(append([]byte("kelvo.operation.adapter-tenant.v1\x00"), identity...))
	return err == nil && authority == binding.Authority && input.Source.TenantID == hex.EncodeToString(tenant[:]) && input.AppTeam == binding.Tenant && input.OperationID == binding.Execution.ID && input.Source.ConnectionID == binding.Source && input.Source.Revision == binding.SourceRevision && binding.Execution.Kind == "operation" && input.ExpiresAt <= binding.ExpiresAt.Unix()
}
