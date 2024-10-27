package main

import (
	"fmt"
	"log/slog"
	"net"
)

const (
	APIVersion              = 18
	DescribeTopicPartitions = 75
)

func Handle(conn net.Conn) error {
	readBuffer := make([]byte, 1024)
	_, err := conn.Read(readBuffer)
	if err != nil {
		return err
	}

	header, readBuffer, err := parseRequestHeaderV1(readBuffer)
	if err != nil {
		slog.Error("parse request header", "err", err)
		return err
	}

	var response []byte
	switch header.RequestAPIKey {
	case APIVersion:
		response, err = handleAPIVersion(readBuffer, header)
		if err != nil {
			return err
		}
	case DescribeTopicPartitions:
		response, err = handleDescribeTopicPartitions(readBuffer, header)
		if err != nil {
			return err
		}
	}

	_, err = conn.Write(response)
	return err
}

func handleAPIVersion(_ []byte, header *RequestHeaderV1) ([]byte, error) {
	var resp *APIVersionResponse
	if header.RequestAPIVersion > 4 || header.RequestAPIVersion < 0 {
		resp = &APIVersionResponse{
			ResponseHeader: ResponseHeader{
				CorrelationID: header.CorrelationID,
			},
			ErrorCode: ErrCodeUnsupportedAPIVersion,
		}
	} else {
		resp = &APIVersionResponse{
			ResponseHeader: ResponseHeader{
				CorrelationID: header.CorrelationID,
			},
			ErrorCode:      0,
			ThrottleTimeMS: 1000,
			APIKeys: []APIKey{
				{
					APIKey:     APIVersion,
					MinVersion: 0,
					MaxVersion: 4,
				},
				{
					APIKey:     DescribeTopicPartitions,
					MinVersion: 0,
					MaxVersion: 0,
				},
			},
		}
	}
	return resp.Serialize(), nil
}

func handleDescribeTopicPartitions(body []byte, header *RequestHeaderV1) ([]byte, error) {
	req, err := parseDescribeTopicPartitionsRequestBodyV1(body)
	if err != nil {
		return nil, fmt.Errorf("handle describe topic partitions: %w", err)
	}

	resp := &DescribeTopicPartitionsResponseBodyV1{
		ResponseHeader: ResponseHeader{
			CorrelationID: header.CorrelationID,
		},
		ThrottleTimeMS: 0,
		NextCursor:     0,
	}
	var topics []Topic
	for _, topic := range req.Names {
		topics = append(topics, Topic{
			ErrorCode:                 ErrCodeUnknownTopicOrPartition,
			TopicName:                 topic,
			TopicID:                   [16]byte{},
			IsInternal:                0,
			PartitionsArray:           0,
			TopicAuthorizedOperations: 0,
		})
	}
	resp.Topics = topics

	return resp.Serialize(), nil
}
