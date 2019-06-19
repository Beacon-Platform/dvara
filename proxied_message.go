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
  if message.parts == nil {
		message.loadParts()
	}
	return message.parts, message.err
}

func (message* ProxiedMessage) GetFullCollectionName() ([]byte, error) {
  if message.fullCollectionName == nil {
    message.loadParts()
  }
	return message.fullCollectionName, message.err
}

func (message* ProxiedMessage) GetQueryDoc() ([]byte, error) {
  if message.queryDoc == nil {
    message.loadQuery()
  }
	return message.queryDoc, message.err
}

func (message* ProxiedMessage) GetQuery() (*bson.D, error) {
  if message.query == nil {
    if message.queryDoc == nil {
      if message.header.OpCode != OpQuery {
        return nil, nil
      }
      if _, err := message.GetQueryDoc(); err != nil {
        return nil, err
      }
    }
    message.err = bson.Unmarshal(message.queryDoc, &message.query);
  }
  return &message.query, message.err
}

func (message* ProxiedMessage) loadParts() error {
  if message.parts != nil {
    return nil
  }
  if message.err != nil {
    return message.err
  }

	message.parts = [][]byte{message.header.ToWire()}
	var err error

	var flags [4]byte
	if _, err := io.ReadFull(message.client, flags[:]); err != nil {
    message.err = err
		corelog.LogError("error", err)
		return err
	}
	message.parts = append(message.parts, flags[:])

	message.fullCollectionName, err = readCString(message.client)
	if err != nil {
    message.err = err
		corelog.LogError("error", err)
		return err
	}
	message.parts = append(message.parts, message.fullCollectionName)

	return nil
}

func (message* ProxiedMessage) loadQuery() error {
  if err := message.loadParts(); err != nil {
		return err
	}

  if message.queryDoc == nil {
    var twoInt32 [8]byte
    if _, err := io.ReadFull(message.client, twoInt32[:]); err != nil {
      message.err = err
      corelog.LogError("error", err)
      return err
    }
    message.parts = append(message.parts, twoInt32[:])

    queryDoc, err := readDocument(message.client)
    if err != nil {
      message.err = err
      corelog.LogError("error", err)
      return err
    }
    message.queryDoc = queryDoc
    message.parts = append(message.parts, message.queryDoc)
  }
  return nil
}
