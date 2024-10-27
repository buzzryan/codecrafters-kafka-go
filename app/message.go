package main

import (
	"encoding/binary"
	"errors"
)

type RequestHeaderV1 struct {
	Length            int32
	RequestAPIKey     int16
	RequestAPIVersion int16
	CorrelationID     int32
	ClientIDLength    int16
	ClientID          []byte
	TagBuffer         byte
}

var (
	ErrCodeUnknownTopicOrPartition int16 = 3
	ErrCodeUnsupportedAPIVersion   int16 = 35
)

type TopicName struct {
	Length int8
	Value  []byte
}

type DescribeTopicPartitionsRequestBodyV1 struct {
	ArrayLength            int8
	Names                  []TopicName
	ResponsePartitionLimit int32
	Cursor                 int8
	TagBuffer              byte
}

func parseRequestHeaderV1(buf []byte) (*RequestHeaderV1, []byte, error) {
	if len(buf) < 15 {
		return nil, buf, errors.New("invalid request header")
	}
	rh := &RequestHeaderV1{
		Length:            int32(binary.BigEndian.Uint32(buf[0:4])),
		RequestAPIKey:     int16(binary.BigEndian.Uint16(buf[4:6])),
		RequestAPIVersion: int16(binary.BigEndian.Uint16(buf[6:8])),
		CorrelationID:     int32(binary.BigEndian.Uint32(buf[8:12])),
	}
	clientIDLength := int16(binary.BigEndian.Uint16(buf[12:14]))
	rh.ClientIDLength = clientIDLength

	if len(buf[14:]) < int(rh.ClientIDLength)+1 {
		return nil, buf, errors.New("invalid request header")
	}
	rh.ClientID = buf[14 : 14+int(rh.ClientIDLength)]

	return rh, buf[15+int(rh.ClientIDLength):], nil
}

func parseDescribeTopicPartitionsRequestBodyV1(buf []byte) (*DescribeTopicPartitionsRequestBodyV1, error) {
	arrayLength := int8(buf[0]) - 1
	var names []TopicName
	unreadBuf := buf[1:]
	for i := 0; i < int(arrayLength); i++ {
		length := int8(unreadBuf[0])
		names = append(names, TopicName{
			Length: length,
			Value:  unreadBuf[1:length],
		})
		unreadBuf = unreadBuf[1+length:] // buffer
	}
	responsePartitionLimit := int32(binary.BigEndian.Uint32(unreadBuf[:4]))
	cursor := int8(unreadBuf[4])

	return &DescribeTopicPartitionsRequestBodyV1{
		ArrayLength:            arrayLength,
		Names:                  names,
		ResponsePartitionLimit: responsePartitionLimit,
		Cursor:                 cursor,
		TagBuffer:              0,
	}, nil
}

type ResponseHeader struct {
	CorrelationID int32
}

type APIVersionResponse struct {
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

func (r *APIVersionResponse) Serialize() []byte {
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

type Topic struct {
	ErrorCode                 int16
	TopicName                 TopicName
	TopicID                   [16]byte
	IsInternal                byte
	PartitionsArray           byte
	TopicAuthorizedOperations int32
}

type DescribeTopicPartitionsResponseBodyV1 struct {
	ResponseHeader
	ThrottleTimeMS int32
	Topics         []Topic
	NextCursor     byte
}

func (d *DescribeTopicPartitionsResponseBodyV1) Serialize() []byte {
	var b []byte
	b = binary.BigEndian.AppendUint32(b, uint32(d.ResponseHeader.CorrelationID))

	b = append(b, 0) // add Tag Buffer
	b = binary.BigEndian.AppendUint32(b, uint32(d.ThrottleTimeMS))
	b = append(b, byte(len(d.Topics)+1)) // add array length

	for _, topic := range d.Topics {
		b = binary.BigEndian.AppendUint16(b, uint16(topic.ErrorCode))

		b = append(b, byte(topic.TopicName.Length))
		b = append(b, topic.TopicName.Value...)
		b = append(b, topic.TopicID[:]...)
		b = append(b, topic.IsInternal)
		b = append(b, topic.PartitionsArray)
		b = binary.BigEndian.AppendUint32(b, uint32(topic.TopicAuthorizedOperations))

		b = append(b, byte(0))
	}

	b = append(b, d.NextCursor)
	b = append(b, byte(0))

	var fullResp []byte
	fullResp = binary.BigEndian.AppendUint32(fullResp, uint32(len(b)))
	return append(fullResp, b...)
}
