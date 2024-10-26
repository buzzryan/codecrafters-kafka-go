package main

import (
	"encoding/binary"
	"errors"
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

	conn, err := l.Accept()
	if err != nil {
		slog.Error("accept connection: ", err.Error())
		os.Exit(1)
	}
	defer func(conn net.Conn) {
		err := conn.Close()
		if err != nil {
			slog.Warn("close connection: ", err.Error())
		}
	}(conn)

	for {
		readBuffer := make([]byte, 1024)
		_, err = conn.Read(readBuffer)
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

type request struct {
	RequestHeaderV1
}

type RequestHeaderV1 struct {
	Length            int32
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
}

func parseRequest(buf []byte) (*request, error) {
	if len(buf) < 12 {
		return nil, errors.New("buffer too small")
	}
	return &request{
		RequestHeaderV1: RequestHeaderV1{
			Length:            int32(binary.BigEndian.Uint32(buf[0:4])),
			RequestAPIKey:     int16(binary.BigEndian.Uint16(buf[4:6])),
			RequestAPIVersion: int16(binary.BigEndian.Uint16(buf[6:8])),
			CorrelationID:     int32(binary.BigEndian.Uint32(buf[8:12])),
		},
	}, nil
}

type Response struct {
	ResponseHeader
	ErrorCode      int16
	ThrottleTimeMS int32
	APIKeys        []APIKey
}

type APIKey struct {
	APIKey     int16
	MinVersion int16
	MaxVersion int16
}

type ResponseHeader struct {
	CorrelationID int32
}

func (r *Response) Serialize() []byte {
	var b []byte
	b = binary.BigEndian.AppendUint32(b, uint32(r.ResponseHeader.CorrelationID))
	b = binary.BigEndian.AppendUint16(b, uint16(r.ErrorCode))

	if r.ErrorCode == 0 {
		b = append(b, byte(2))
		for _, apiKey := range r.APIKeys {
			b = binary.BigEndian.AppendUint16(b, uint16(apiKey.APIKey))
			b = binary.BigEndian.AppendUint16(b, uint16(apiKey.MinVersion))
			b = binary.BigEndian.AppendUint16(b, uint16(apiKey.MaxVersion))
		}

		b = append(b, byte(0))
		b = binary.BigEndian.AppendUint32(b, uint32(r.ThrottleTimeMS))
		b = append(b, byte(0))
	}

	var fullResp []byte
	fullResp = binary.BigEndian.AppendUint32(fullResp, uint32(len(b)))
	return append(fullResp, b...)
}
