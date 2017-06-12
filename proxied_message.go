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
}

func NewProxiedMessage(
		header *messageHeader, client net.Conn,
		server net.Conn, lastError *LastError) ProxiedMessage {
	return ProxiedMessage{
		header, client, server, lastError,
		nil, nil, nil, nil,
	}
}

func (message* ProxiedMessage) GetParts() [][]byte {
	if message.parts == nil {
		message.loadParts()
	}
	return message.parts
}

func (message* ProxiedMessage) GetFullCollectionName() []byte {
  if message.fullCollectionName == nil {
		message.loadParts()
	}
	return message.fullCollectionName
}

func (message* ProxiedMessage) GetQueryDoc() []byte {
	if message.queryDoc == nil {
		message.loadParts()
	}
	return message.queryDoc
}

func (message* ProxiedMessage) GetQuery() bson.D {
  if message.query == nil {
		message.loadQuery()
	}
	return message.query
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
	if err := bson.Unmarshal(message.GetQueryDoc(), &message.query); err != nil {
		corelog.LogError("error", err)
		return err
	}
	return nil
}