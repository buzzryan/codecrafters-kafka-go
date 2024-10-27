package main

import (
	"encoding/binary"
	"errors"
)

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
		b = append(b, byte(len(r.APIKeys)+1))
		for _, apiKey := range r.APIKeys {
			b = binary.BigEndian.AppendUint16(b, uint16(apiKey.APIKey))
			b = binary.BigEndian.AppendUint16(b, uint16(apiKey.MinVersion))
			b = binary.BigEndian.AppendUint16(b, uint16(apiKey.MaxVersion))
			b = append(b, byte(0))
		}

		b = binary.BigEndian.AppendUint32(b, uint32(r.ThrottleTimeMS))
		b = append(b, byte(0))
	}

	var fullResp []byte
	fullResp = binary.BigEndian.AppendUint32(fullResp, uint32(len(b)))
	return append(fullResp, b...)
}
