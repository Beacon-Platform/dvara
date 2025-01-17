package dvara

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	corelog "github.com/intercom/gocore/log"
	"gopkg.in/mgo.v2/bson"
)

var (
	proxyAllQueries = flag.Bool(
		"dvara.proxy-all",
		false,
		"if true all queries will be proxied and logged",
	)
	readOnly = flag.Bool(
		"dvara.readonly",
		false,
		"if true only readonly queries will be allowed",
	)

	adminCollectionName = []byte("admin.$cmd\000")
	cmdCollectionSuffix = []byte(".$cmd\000")
)

//https: //github.com/mongodb/mongo/blob/master/src/mongo/base/error_codes.err#L16
const authErrorCode = 13

// ProxyQuery proxies an OpQuery and a corresponding response.
type ProxyQuery struct {
	GetLastErrorRewriter             *GetLastErrorRewriter             `inject:""`
	IsMasterResponseRewriter         *IsMasterResponseRewriter         `inject:""`
	ReplSetGetStatusResponseRewriter *ReplSetGetStatusResponseRewriter `inject:""`
}

// Proxy proxies an OpQuery and a corresponding response.
func (p *ProxyQuery) Proxy(message *ProxiedMessage) error {

	// https://github.com/mongodb/mongo/search?q=lastError.disableForCommand
	// Shows the logic we need to be in sync with. Unfortunately it isn't a
	// simple check to determine this, and may change underneath us at the mongo
	// layer.
	resetLastError := true
	fullCollectionName, err1 := message.GetFullCollectionName()
	if err1 != nil {
		return err1
	}

	var rewriter responseRewriter
	if *proxyAllQueries || *readOnly || bytes.HasSuffix(fullCollectionName, cmdCollectionSuffix) {
		q, err3 := message.GetQuery()
		if err3 != nil {
			return err3
		}

		if *readOnly {
			if hasKey(*q, "insert") || hasKey(*q, "delete") || hasKey(*q, "update") {
				message.lastError.NewError("Readonly database", 66)
				err := p.GetLastErrorRewriter.Rewrite(message)
				message.lastError.Reset()
				return err
			}
		}

    if q != nil {
      if hasKey(*q, "getLastError") {
        return p.GetLastErrorRewriter.Rewrite(message)
      }

      if hasKey(*q, "isMaster") {
        rewriter = p.IsMasterResponseRewriter
      }

      if bytes.Equal(adminCollectionName, fullCollectionName) && hasKey(*q, "replSetGetStatus") {
        rewriter = p.ReplSetGetStatusResponseRewriter
      }
    }

		if rewriter != nil {
			// If forShell is specified, we don't want to reset the last error. See
			// comment above around resetLastError for details.
			resetLastError = hasKey(*q, "forShell")
		}
	}

	if resetLastError && message.lastError.Exists() {
		corelog.LogInfoMessage("reset getLastError cache")
		message.lastError.Reset()
	}

	parts, err2 := message.GetParts()
	if err2 != nil {
		return err2
	}

	var written int
	for _, b := range parts {
		n, err := message.server.Write(b)
		if err != nil {
			corelog.LogError("error", err)
			return err
		}
		written += n
	}

	pending := int64(message.header.MessageLength) - int64(written)
	if _, err := io.CopyN(message.server, message.client, pending); err != nil {
		corelog.LogError("error", err)
		return err
	}

	if rewriter != nil {
		if err := rewriter.Rewrite(message.client, message.server); err != nil {
			return err
		}
		return nil
	}

	if err := copyMessage(message.client, message.server); err != nil {
		corelog.LogError("error", err)
		return err
	}

	return nil
}

// LastError holds the last known error.
type LastError struct {
	header *messageHeader
	rest   bytes.Buffer
}

// Exists returns true if this instance contains a cached error.
func (l *LastError) Exists() bool {
	return l.header != nil
}

// Reset resets the stored error clearing it.
func (l *LastError) Reset() {
	l.header = nil
	l.rest.Reset()
}

// Creates an error
func (l *LastError) NewError(msg string, code int) error {
	errDoc := bson.M{"$err": msg, "code": code}
	data, err := bson.Marshal(errDoc)
	if err != nil {
		return err
	}
	l.rest.Reset()
	if _, err = l.rest.Write([]byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}); err != nil {
		return err
	}
	if _, err = l.rest.Write(data); err != nil {
		return err
	}
	dataLen := int32(l.rest.Len())
	l.header = &messageHeader{
		MessageLength: headerLen + dataLen,
		RequestID:     1,
		ResponseTo:    0,
		OpCode:        OpReply,
	}
	return nil
}

// GetLastErrorRewriter handles getLastError requests and proxies, caches or
// sends cached responses as necessary.
type GetLastErrorRewriter struct {
}

// Rewrite handles getLastError requests.
func (r *GetLastErrorRewriter) Rewrite(
	m *ProxiedMessage,
) error {
	h := m.header
	client := m.client
	server := m.server
	lastError := m.lastError
	parts, _ := m.GetParts()

	if !lastError.Exists() {
		// We're going to be performing a real getLastError query and caching the
		// response.
		var written int
		for _, b := range parts {
			n, err := server.Write(b)
			if err != nil {
				corelog.LogError("error", err)
				return err
			}
			written += n
		}

		pending := int64(h.MessageLength) - int64(written)
		if _, err := io.CopyN(server, client, pending); err != nil {
			corelog.LogError("error", err)
			return err
		}

		var err error
		if lastError.header, err = readHeader(server); err != nil {
			corelog.LogError("error", err)
			return err
		}
		pending = int64(lastError.header.MessageLength - headerLen)
		if _, err = io.CopyN(&lastError.rest, server, pending); err != nil {
			corelog.LogError("error", err)
			return err
		}
		corelog.LogInfoMessage(fmt.Sprintf("caching new getLastError response: %s", lastError.rest.Bytes()))
	} else {
		// We need to discard the pending bytes from the client from the query
		// before we send it our cached response.
		var written int
		for _, b := range parts {
			written += len(b)
		}
		pending := int64(h.MessageLength) - int64(written)
		if _, err := io.CopyN(ioutil.Discard, client, pending); err != nil {
			corelog.LogError("error", err)
			return err
		}
		// Modify and send the cached response for this request.
		lastError.header.ResponseTo = h.RequestID
		corelog.LogInfoMessage("using cached getLastError response: %s", lastError.rest.Bytes())
	}

	if err := lastError.header.WriteTo(client); err != nil {
		corelog.LogError("error", err)
		return err
	}
	if _, err := client.Write(lastError.rest.Bytes()); err != nil {
		corelog.LogError("error", err)
		return err
	}

	return nil
}

var errRSChanged = errors.New("dvara: replset config changed")

// ProxyMapper maps real mongo addresses to their corresponding proxy
// addresses.
type ProxyMapper interface {
	Proxy(h string) (string, error)
}

type responseRewriter interface {
	Rewrite(client io.Writer, server io.Reader) error
}

type replyPrefix [20]byte

var emptyPrefix replyPrefix

// ReplyRW provides common helpers for rewriting replies from the server.
type ReplyRW struct {
}

// ReadOne reads a 1 document response, from the server, unmarshals it into v
// and returns the various parts.
func (r *ReplyRW) ReadOne(server io.Reader, v interface{}) (*messageHeader, replyPrefix, int32, error) {
	h, err := readHeader(server)
	if err != nil {
		corelog.LogError("error", err)
		return nil, emptyPrefix, 0, err
	}

	if h.OpCode != OpReply {
		err := fmt.Errorf("readOneReplyDoc: expected op %s, got %s", OpReply, h.OpCode)
		return nil, emptyPrefix, 0, err
	}

	var prefix replyPrefix
	if _, err := io.ReadFull(server, prefix[:]); err != nil {
		corelog.LogError("error", err)
		return nil, emptyPrefix, 0, err
	}

	numDocs := getInt32(prefix[:], 16)
	if numDocs != 1 {
		err := fmt.Errorf("readOneReplyDoc: can only handle 1 result document, got: %d", numDocs)
		return nil, emptyPrefix, 0, err
	}

	rawDoc, err := readDocument(server)
	if err != nil {
		corelog.LogError("error", err)
		return nil, emptyPrefix, 0, err
	}

	if err := bson.Unmarshal(rawDoc, v); err != nil {
		corelog.LogError("error", err)
		return nil, emptyPrefix, 0, err
	}

	return h, prefix, int32(len(rawDoc)), nil
}

// WriteOne writes a rewritten response to the client.
func (r *ReplyRW) WriteOne(client io.Writer, h *messageHeader, prefix replyPrefix, oldDocLen int32, v interface{}) error {
	newDoc, err := bson.Marshal(v)
	if err != nil {
		return err
	}

	h.MessageLength = h.MessageLength - oldDocLen + int32(len(newDoc))
	parts := [][]byte{h.ToWire(), prefix[:], newDoc}
	for _, p := range parts {
		if _, err := client.Write(p); err != nil {
			return err
		}
	}

	return nil
}

type isMasterResponse struct {
	Arbiters []string `bson:"arbiters,omitempty"`
	Hosts    []string `bson:"hosts,omitempty"`
	Primary  string   `bson:"primary,omitempty"`
	Me       string   `bson:"me,omitempty"`
	Extra    bson.M   `bson:",inline"`
}

// IsMasterResponseRewriter rewrites the response for the "isMaster" query.
type IsMasterResponseRewriter struct {
	ProxyMapper ProxyMapper `inject:""`
	ReplyRW     *ReplyRW    `inject:""`
}

// Rewrite rewrites the response for the "isMaster" query.
func (r *IsMasterResponseRewriter) Rewrite(client io.Writer, server io.Reader) error {
	var err error
	var q isMasterResponse
	h, prefix, docLen, err := r.ReplyRW.ReadOne(server, &q)
	if err != nil {
		return err
	}

	// skip the arbiter host
	q.Arbiters = []string{}

	var newHosts []string
	for _, h := range q.Hosts {
		newH, err := r.ProxyMapper.Proxy(h)

		if err != nil {
			continue
		}
		newHosts = append(newHosts, newH)
	}
	q.Hosts = newHosts

	var newPassives []string
	passives, ok := q.Extra["passives"].([]interface{})

	if ok {
		for _, p := range passives {
			newP, err := r.ProxyMapper.Proxy(p.(string))
			if err != nil {
				continue
			}
			newPassives = append(newPassives, newP)
		}
		q.Extra["passives"] = newPassives
	}

	if q.Primary != "" {
		// failure in mapping the primary is fatal
		if q.Primary, err = r.ProxyMapper.Proxy(q.Primary); err != nil {
			return err
		}
	}
	if q.Me != "" {
		// failure in mapping me is fatal
		if q.Me, err = r.ProxyMapper.Proxy(q.Me); err != nil {
			return err
		}
	}

	return r.ReplyRW.WriteOne(client, h, prefix, docLen, q)
}

type statusMember struct {
	Name  string       `bson:"name"`
	State ReplicaState `bson:"stateStr,omitempty"`
  StateCode int      `bson:"state"`
	Self  bool         `bson:"self,omitempty"`
	Extra bson.M       `bson:",inline"`
}

type replSetGetStatusResponse struct {
	Name    string                 `bson:"set,omitempty"`
	Members []statusMember         `bson:"members"`
	Extra   map[string]interface{} `bson:",inline"`
}

// ReplSetGetStatusResponseRewriter rewrites the "replSetGetStatus" response.
type ReplSetGetStatusResponseRewriter struct {
	ProxyMapper ProxyMapper `inject:""`
	ReplyRW     *ReplyRW    `inject:""`
}

// Rewrite rewrites the "replSetGetStatus" response.
func (r *ReplSetGetStatusResponseRewriter) Rewrite(client io.Writer, server io.Reader) error {
	var err error
	var q replSetGetStatusResponse
	h, prefix, docLen, err := r.ReplyRW.ReadOne(server, &q)
	if err != nil {
		return err
	}

	code := q.Extra["code"]
	if code == authErrorCode {
		return fmt.Errorf("Authentication error, more info in %s", q.Extra)
	}

	var newMembers []statusMember
	for _, m := range q.Members {
		newH, err := r.ProxyMapper.Proxy(m.Name)
		if err != nil {
			continue
		}
		m.Name = newH
		newMembers = append(newMembers, m)
	}
	q.Members = newMembers
	return r.ReplyRW.WriteOne(client, h, prefix, docLen, q)
}

// case insensitive check for the specified key name in the top level.
func hasKey(d bson.D, k string) bool {
	for _, v := range d {
		if strings.EqualFold(v.Name, k) {
			return true
		}
	}
	return false
}
