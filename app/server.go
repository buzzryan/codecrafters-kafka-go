package main

import (
	"log/slog"
	"net"
	"os"
)

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		slog.Info("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			slog.Error("accept connection: ", err.Error())
			os.Exit(1)
		}
		go func(cn net.Conn) {
			accept(cn)

			err = conn.Close()
			if err != nil {
				slog.Error("close connection: ", err.Error())
			}
		}(conn)
	}
}

func accept(conn net.Conn) {
	for {
		if err := Handle(conn); err != nil {
			slog.Error("handle connection: ", err.Error())
			break
		}
	}
}
