package main

import (
	"io"
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
		readBuffer := make([]byte, 1024)
		_, err := conn.Read(readBuffer)
		if err == io.EOF {
			return
		}
		if err != nil {
			slog.Error("read from the connected stream: ", err.Error())
			os.Exit(1)
		}
		req, err := parseRequest(readBuffer)
		if err != nil {
			slog.Error("parse request: ", err.Error())
			os.Exit(1)
		}

		var resp *Response
		if req.RequestAPIVersion > 4 || req.RequestAPIVersion < 0 {
			resp = &Response{
				ResponseHeader: ResponseHeader{
					CorrelationID: req.CorrelationID,
				},
				ErrorCode: 35,
			}
		} else {
			resp = &Response{
				ResponseHeader: ResponseHeader{
					CorrelationID: req.CorrelationID,
				},
				ErrorCode:      0,
				ThrottleTimeMS: 1000,
				APIKeys: []APIKey{
					{
						APIKey:     18,
						MinVersion: 0,
						MaxVersion: 4,
					},
					{
						APIKey:     75,
						MinVersion: 0,
						MaxVersion: 0,
					},
				},
			}
		}

		_, err = conn.Write(resp.Serialize())
		if err != nil {
			slog.Error("write response: ", err.Error())
			os.Exit(1)
		}
	}
}
