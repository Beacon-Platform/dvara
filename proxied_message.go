package dvara

import (
	"net"
	"gopkg.in/mgo.v2/bson"
	"io"
	corelog "github.com/intercom/gocore/log"
)

type ProxiedMessage struct {
	header *messageHeader
	client net.Conn
	server net.Conn
	lastError *LastError

	parts [][]byte
  fullCollectionName []byte
	queryDoc []byte
  query bson.D

  err error
}

func NewProxiedMessage(
		header *messageHeader, client net.Conn,
		server net.Conn, lastError *LastError) ProxiedMessage {
	return ProxiedMessage{
		header, client, server, lastError,
		nil, nil, nil, nil,
		nil,
	}
}

func (message* ProxiedMessage) GetParts() ([][]byte, error) {
	if message.err != nil {
		return nil, message.err
	} else if message.parts == nil {
		message.err = message.loadParts()
	}
	return message.parts, message.err
}

func (message* ProxiedMessage) GetFullCollectionName() ([]byte, error) {
	if message.err != nil {
		return nil, message.err
	} else if message.fullCollectionName == nil {
		message.err = message.loadParts()
	}
	return message.fullCollectionName, message.err
}

func (message* ProxiedMessage) GetQueryDoc() ([]byte, error) {
	if message.err != nil {
		return nil, message.err
	} else if message.queryDoc == nil {
		message.err = message.loadParts()
	}
	return message.queryDoc, message.err
}

func (message* ProxiedMessage) GetQuery() (bson.D, error) {
	if message.err != nil {
		return nil, message.err
	} else if message.query == nil {
		message.err = message.loadQuery()
	}
	return message.query, message.err
}

func (message* ProxiedMessage) loadParts() error {
	message.parts = [][]byte{message.header.ToWire()}
	var err error

	var flags [4]byte
	if _, err := io.ReadFull(message.client, flags[:]); err != nil {
		corelog.LogError("error", err)
		return err
	}
	message.parts = append(message.parts, flags[:])

	message.fullCollectionName, err = readCString(message.client)
	if err != nil {
		corelog.LogError("error", err)
		return err
	}
	message.parts = append(message.parts, message.fullCollectionName)

	var twoInt32 [8]byte
	if _, err := io.ReadFull(message.client, twoInt32[:]); err != nil {
		corelog.LogError("error", err)
		return err
	}
	message.parts = append(message.parts, twoInt32[:])

	message.queryDoc, err = readDocument(message.client)
	if err != nil {
		corelog.LogError("error", err)
		return err
	}
	message.parts = append(message.parts, message.queryDoc)
	return nil
}

func (message* ProxiedMessage) loadQuery() error {
	if message.err != nil {
		return message.err
	}	else {
	  var queryDoc []byte
		queryDoc, message.err = message.GetQueryDoc()
		if message.err != nil {
			return message.err
		}
	  message.err = bson.Unmarshal(queryDoc, &message.query);
	  if message.err != nil {
			return message.err
	  }
	}
	return nil
}
