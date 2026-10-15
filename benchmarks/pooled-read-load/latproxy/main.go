// Copyright 2026 SYNEHQ. SPDX-License-Identifier: Apache-2.0

// Command latproxy forwards TCP with a fixed one-way delay in each direction,
// standing in for a source database a WAN away. It delays every chunk by the
// same amount, in order, so a request/response exchange costs two delays (one
// round trip), and delays the connection itself by one round trip, like a TCP
// handshake. It is not tc netem: it touches only the connections it carries and
// needs no privileges, so the node's own loopback traffic is not delayed.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"os"
	"sync/atomic"
	"time"
)

type chunk struct {
	data []byte
	due  time.Time
}

func pump(dst net.Conn, src net.Conn, delay time.Duration, bytes *atomic.Int64) {
	queue := make(chan chunk, 4096)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for c := range queue {
			if wait := time.Until(c.due); wait > 0 {
				time.Sleep(wait)
			}
			if _, err := dst.Write(c.data); err != nil {
				for range queue {
				}
				return
			}
		}
	}()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			bytes.Add(int64(n))
			queue <- chunk{data: append([]byte(nil), buf[:n]...), due: time.Now().Add(delay)}
		}
		if err != nil {
			break
		}
	}
	close(queue)
	<-done
	if c, ok := dst.(*net.TCPConn); ok {
		_ = c.CloseWrite()
	}
}

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "address to listen on")
	upstream := flag.String("upstream", "", "address to forward to")
	oneWay := flag.Duration("one-way", 40*time.Millisecond, "delay added in each direction")
	ready := flag.String("ready-file", "", "write the bound address here once listening")
	stats := flag.String("stats-file", "", "write connection and byte counts here on SIGTERM")
	flag.Parse()
	if *upstream == "" {
		log.Fatal("-upstream is required")
	}
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	if *ready != "" {
		if err := os.WriteFile(*ready, []byte(ln.Addr().String()), 0600); err != nil {
			log.Fatal(err)
		}
	}
	var conns, toUpstream, toClient atomic.Int64
	if *stats != "" {
		go func() {
			for range time.Tick(time.Second) {
				raw, _ := json.Marshal(map[string]any{"connections": conns.Load(), "bytes_to_source": toUpstream.Load(), "bytes_from_source": toClient.Load(), "one_way_ms": oneWay.Milliseconds()})
				_ = os.WriteFile(*stats, raw, 0600)
			}
		}()
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		conns.Add(1)
		go func() {
			defer c.Close()
			time.Sleep(2 * *oneWay) // the handshake round trip
			u, err := net.Dial("tcp", *upstream)
			if err != nil {
				return
			}
			defer u.Close()
			done := make(chan struct{}, 2)
			go func() { pump(u, c, *oneWay, &toUpstream); done <- struct{}{} }()
			go func() { pump(c, u, *oneWay, &toClient); done <- struct{}{} }()
			<-done
			<-done
		}()
	}
}
