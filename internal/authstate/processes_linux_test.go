//go:build linux

// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0
package authstate

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestAuthstateSubprocessHelper(t *testing.T) {
	mode := os.Getenv("KELVO_AUTHSTATE_HELPER")
	if mode == "" {
		return
	}
	directory := os.Getenv("KELVO_AUTHSTATE_DIR")
	switch mode {
	case "hold":
		store, err := Open(context.Background(), directory, testScope())
		if err != nil {
			os.Exit(71)
		}
		fmt.Println("LOCKED")
		if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
			os.Exit(72)
		}
		if err := store.Close(context.Background()); err != nil {
			os.Exit(72)
		}
		os.Exit(0)
	case "crash":
		stage := os.Getenv("KELVO_AUTHSTATE_STAGE")
		operations := defaultIO()
		operations.checkpoint = func(reached string) {
			if reached == stage {
				os.Exit(73)
			}
		}
		store, err := openWithIO(context.Background(), directory, testScope(), nil, operations)
		if err != nil {
			os.Exit(71)
		}
		_ = store.Admit(context.Background(), testCandidate(2, "retained", "next"))
		os.Exit(74) // no crash point reached
	default:
		os.Exit(75)
	}
}

func authstateCommand(t *testing.T, mode, directory, stage string) *exec.Cmd {
	t.Helper()
	executable, err := os.Executable()
	requireOK(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	command := exec.CommandContext(ctx, executable, "-test.run=^TestAuthstateSubprocessHelper$")
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "KELVO_AUTHSTATE_HELPER=") && !strings.HasPrefix(entry, "KELVO_AUTHSTATE_DIR=") && !strings.HasPrefix(entry, "KELVO_AUTHSTATE_STAGE=") {
			command.Env = append(command.Env, entry)
		}
	}
	command.Env = append(command.Env, "KELVO_AUTHSTATE_HELPER="+mode, "KELVO_AUTHSTATE_DIR="+directory, "KELVO_AUTHSTATE_STAGE="+stage)
	return command
}

func TestStoreProcessLockSurvivesCompetingOpenAndInitialization(t *testing.T) {
	directory, initial := initialized(t)
	command := authstateCommand(t, "hold", directory, "")
	var diagnostic bytes.Buffer
	command.Stderr = &diagnostic
	input, err := command.StdinPipe()
	requireOK(t, err)
	output, err := command.StdoutPipe()
	requireOK(t, err)
	requireOK(t, command.Start())
	waited := false
	t.Cleanup(func() {
		_ = input.Close()
		if !waited {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	line, err := bufio.NewReader(output).ReadString('\n')
	if err != nil || line != "LOCKED\n" {
		t.Fatal("child did not acquire the writer lock", err)
	}
	if store, err := Open(context.Background(), directory, testScope()); store != nil || err != ErrUnavailable {
		t.Fatal("competing process acquired the lock", err)
	}
	if err := Initialize(context.Background(), directory, testScope(), testCandidate(9, "replacement")); err != ErrUnavailable {
		t.Fatal("initializer replaced an active process history", err)
	}
	assertLockHeld(t, directory)
	requireOK(t, input.Close())
	err = command.Wait()
	waited = true
	if err != nil {
		t.Fatal("lock holder failed to close", err, diagnostic.String())
	}
	store := waitReopen(t, directory)
	requireOK(t, store.Admit(context.Background(), initial))
	requireOK(t, store.Admit(context.Background(), testCandidate(2, "next")))
}

func TestStoreCrashRecoveryAtPublicationBoundaries(t *testing.T) {
	for _, stage := range []string{"before-write", "staging-synced", "renamed", "directory-synced"} {
		t.Run(stage, func(t *testing.T) {
			directory, initial := initialized(t)
			command := authstateCommand(t, "crash", directory, stage)
			var output bytes.Buffer
			command.Stdout, command.Stderr = &output, &output
			err := command.Run()
			var exited *exec.ExitError
			if !errors.As(err, &exited) || exited.ExitCode() != 73 {
				t.Fatal("child did not stop at the expected crash point", err, output.String())
			}
			store := openTest(t, directory)
			next := testCandidate(2, "retained", "next")
			if stage == "before-write" || stage == "staging-synced" {
				requireOK(t, store.Admit(context.Background(), initial))
			} else {
				if err := store.Admit(context.Background(), initial); err != ErrRejected {
					t.Fatal("crash lost newer visible revision", err)
				}
			}
			requireOK(t, store.Admit(context.Background(), next))
			requireOK(t, store.Close(context.Background()))
			reopened := openTest(t, directory)
			if err := reopened.Admit(context.Background(), initial); err != ErrRejected {
				t.Fatal("recovery did not retain durable revision", err)
			}
			requireOK(t, reopened.Admit(context.Background(), next))
		})
	}
}
