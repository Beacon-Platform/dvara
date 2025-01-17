package dvara

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/facebookgo/ensure"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"

	"gopkg.in/mgo.v2/bson"
)

var errInvalidBSON = errors.New("invalid BSON")

type invalidBSON int

func (i invalidBSON) GetBSON() (interface{}, error) {
	return nil, errInvalidBSON
}

var errProxyNotFound = errors.New("proxy not found")

type fakeProxyMapper struct {
	m map[string]string
}

func (t fakeProxyMapper) Proxy(h string) (string, error) {
	if t.m != nil {
		if r, ok := t.m[h]; ok {
			return r, nil
		}
	}
	return "", errProxyNotFound
}

func fakeReader(h messageHeader, rest []byte) io.Reader {
	return bytes.NewReader(append(h.ToWire(), rest...))
}

func fakeSingleDocReply(v interface{}) io.Reader {
	b, err := bson.Marshal(v)
	if err != nil {
		panic(err)
	}
	b = append(
		[]byte{
			0, 0, 0, 0,
			0, 0, 0, 0, 0, 0, 0, 0,
			0, 0, 0, 0,
			1, 0, 0, 0,
		},
		b...,
	)
	h := messageHeader{
		OpCode:        OpReply,
		MessageLength: int32(headerLen + len(b)),
	}
	return fakeReader(h, b)
}

type fakeReadWriter struct {
	io.Reader
	io.Writer
}

// mock methods to enable net.Conn interface in fakeReadWriter
func (conn fakeReadWriter) Close() error                       { return errors.New("Not implemented") }
func (conm fakeReadWriter) LocalAddr() net.Addr                { return nil }
func (conn fakeReadWriter) RemoteAddr() net.Addr               { return nil }
func (conn fakeReadWriter) SetDeadline(t time.Time) error      { return errors.New("Not implemented") }
func (conn fakeReadWriter) SetReadDeadline(t time.Time) error  { return errors.New("Not implemented") }
func (conn fakeReadWriter) SetWriteDeadline(t time.Time) error { return errors.New("Not implemented") }

func TestResponseRWReadOne(t *testing.T) {
	t.Parallel()
	cases := []struct {
		Name   string
		Server io.Reader
		Error  string
	}{
		{
			Name:   "no header",
			Server: bytes.NewReader(nil),
			Error:  "EOF",
		},
		{
			Name:   "non reply op",
			Server: bytes.NewReader((messageHeader{OpCode: OpDelete}).ToWire()),
			Error:  "expected op REPLY, got DELETE",
		},
		{
			Name:   "EOF before flags",
			Server: bytes.NewReader((messageHeader{OpCode: OpReply}).ToWire()),
			Error:  "EOF",
		},
		{
			Name: "more than 1 document",
			Server: fakeReader(
				messageHeader{OpCode: OpReply},
				[]byte{
					0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0,
					2, 0, 0, 0,
				},
			),
			Error: "can only handle 1 result document, got: 2",
		},
		{
			Name: "EOF before document",
			Server: fakeReader(
				messageHeader{OpCode: OpReply},
				[]byte{
					0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0,
					1, 0, 0, 0,
				},
			),
			Error: "EOF",
		},
		{
			Name: "corrupted document",
			Server: fakeReader(
				messageHeader{OpCode: OpReply},
				[]byte{
					0, 0, 0, 0,
					0, 0, 0, 0, 0, 0, 0, 0,
					0, 0, 0, 0,
					1, 0, 0, 0,
					5, 0, 0, 0,
					1,
				},
			),
			Error: "Document is corrupted",
		},
	}

	for _, c := range cases {
		r := &ReplyRW{}
		m := bson.M{}
		_, _, _, err := r.ReadOne(c.Server, m)
		if err == nil {
			t.Errorf("was expecting an error for case %s", c.Name)
		}
		if !strings.Contains(err.Error(), c.Error) {
			t.Errorf("did not get expected error for case %s instead got %s", c.Name, err)
		}
	}
}

func TestResponseRWWriteOne(t *testing.T) {
	errWrite := errors.New("write error")
	t.Parallel()
	cases := []struct {
		Name   string
		Client io.Writer
		Header messageHeader
		Prefix replyPrefix
		DocLen int32
		Value  interface{}
		Error  string
	}{
		{
			Name:  "invalid bson",
			Value: invalidBSON(0),
			Error: errInvalidBSON.Error(),
		},
		{
			Name:  "write error",
			Value: map[string]string{},
			Client: testWriter{
				write: func(b []byte) (int, error) {
					return 0, errWrite
				},
			},
			Error: errWrite.Error(),
		},
	}

	for _, c := range cases {
		r := &ReplyRW{}
		err := r.WriteOne(c.Client, &c.Header, c.Prefix, c.DocLen, c.Value)
		if err == nil {
			t.Errorf("was expecting an error for case %s", c.Name)
		}
		if !strings.Contains(err.Error(), c.Error) {
			t.Errorf("did not get expected error for case %s instead got %s", c.Name, err)
		}
	}
}

func TestIsMasterResponseRewriterSuccess(t *testing.T) {
	proxyMapper := fakeProxyMapper{
		m: map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		},
	}
	in := bson.M{
		"hosts":    []interface{}{"a", "b", "c"},
		"me":       "a",
		"arbiters": []interface{}{"fooarbiter"},
		"primary":  "b",
		"foo":      "bar",
	}
	out := bson.M{
		"hosts":   []interface{}{"1", "2", "3"},
		"me":      "1",
		"primary": "2",
		"foo":     "bar",
	}

	r := &IsMasterResponseRewriter{
		ProxyMapper: proxyMapper,
		ReplyRW:     &ReplyRW{},
	}

	var client bytes.Buffer
	if err := r.Rewrite(&client, fakeSingleDocReply(in)); err != nil {
		t.Fatal(err)
	}
	actualOut := bson.M{}
	doc := client.Bytes()[headerLen+len(emptyPrefix):]
	if err := bson.Unmarshal(doc, &actualOut); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, actualOut) {
		spew.Dump(out)
		spew.Dump(actualOut)
		t.Fatal("did not get expected output")
	}
}

func TestIsMasterResponseRewriterSuccessWithPassives(t *testing.T) {
	proxyMapper := fakeProxyMapper{
		m: map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		},
	}
	in := bson.M{
		"hosts":    []interface{}{"a", "b", "c"},
		"me":       "a",
		"primary":  "b",
		"foo":      "bar",
		"passives": []interface{}{"a"},
	}
	out := bson.M{
		"hosts":    []interface{}{"1", "2", "3"},
		"me":       "1",
		"primary":  "2",
		"foo":      "bar",
		"passives": []interface{}{"1"},
	}

	r := &IsMasterResponseRewriter{
		ProxyMapper: proxyMapper,
		ReplyRW:     &ReplyRW{},
	}

	var client bytes.Buffer
	if err := r.Rewrite(&client, fakeSingleDocReply(in)); err != nil {
		t.Fatal(err)
	}
	actualOut := bson.M{}
	doc := client.Bytes()[headerLen+len(emptyPrefix):]
	if err := bson.Unmarshal(doc, &actualOut); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, actualOut) {
		spew.Dump(out)
		spew.Dump(actualOut)
		t.Fatal("did not get expected output")
	}
}

func TestReplSetGetStatusResponseRewriterSuccess(t *testing.T) {
	proxyMapper := fakeProxyMapper{
		m: map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		},
	}
	in := bson.M{
		"members": []interface{}{
			bson.M{
				"name":     "a",
				"stateStr": "PRIMARY",
			},
			bson.M{
				"name": "b",
			},
			bson.M{
				"name":     "c",
				"stateStr": "ARBITER",
			},
		},
	}
	out := bson.M{
		"members": []interface{}{
			bson.M{
				"name":     "1",
				"stateStr": "PRIMARY",
			},
			bson.M{
				"name": "2",
			},
			bson.M{
				"name":     "3",
				"stateStr": "ARBITER",
			},
		},
	}
	r := &ReplSetGetStatusResponseRewriter{
		ProxyMapper: proxyMapper,
		ReplyRW:     &ReplyRW{},
	}

	var client bytes.Buffer
	if err := r.Rewrite(&client, fakeSingleDocReply(in)); err != nil {
		t.Fatal(err)
	}
	actualOut := bson.M{}
	doc := client.Bytes()[headerLen+len(emptyPrefix):]
	if err := bson.Unmarshal(doc, &actualOut); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(out, actualOut) {
		spew.Dump(out)
		spew.Dump(actualOut)
		t.Fatal("did not get expected output")
	}
}

func TestReplSetGetStatusResponseRewriterAuthFailure(t *testing.T) {
	proxyMapper := fakeProxyMapper{
		m: map[string]string{
			"a": "1",
			"b": "2",
			"c": "3",
		},
	}
	in := bson.M{
		"code": 13,
	}
	r := &ReplSetGetStatusResponseRewriter{
		ProxyMapper: proxyMapper,
		ReplyRW:     &ReplyRW{},
	}

	var client bytes.Buffer
	if err := r.Rewrite(&client, fakeSingleDocReply(in)); err == nil {
		t.Fatal("Rewrite did not fail, though authentication failed")
	}
}

func TestProxyQuery(t *testing.T) {
	t.Parallel()
	var p ProxyQuery
	var log NoopLogger
	var graph inject.Graph
	err := graph.Provide(
		&inject.Object{Value: &fakeProxyMapper{}},
		&inject.Object{Value: &p},
	)
	ensure.Nil(t, err)
	ensure.Nil(t, graph.Populate())
	objects := graph.Objects()
	ensure.Nil(t, startstop.Start(objects, &log))
	defer startstop.Stop(objects, &log)
	fmt.Printf("Defining cases\n")
  hdr := &messageHeader{OpCode: OpQuery}
	cases := []struct {
		Name   string
		Header *messageHeader
		Client fakeReadWriter
		Error  string
	}{
		{
			Name:   "EOF while reading flags from client",
			Header: &messageHeader{},
			Client: fakeReadWriter{
				Reader: new(bytes.Buffer),
			},
			Error: "EOF",
		},
		{
			Name:   "EOF while reading collection name",
			Header: &messageHeader{},
			Client: fakeReadWriter{
				Reader: bytes.NewReader(
					[]byte{0, 0, 0, 0}, // flags int32 before collection name
				),
			},
			Error: "EOF",
		},
		{
			Name:   "EOF while reading skip/return",
			Header: hdr,
			Client: fakeReadWriter{
				Reader: bytes.NewReader(
					append(
						[]byte{0, 0, 0, 0}, // flags int32 before collection name
						adminCollectionName...,
					),
				),
			},
			Error: "EOF",
		},
		{
			Name:   "EOF while reading query document",
			Header: hdr,
			Client: fakeReadWriter{
				Reader: io.MultiReader(
					bytes.NewReader([]byte{0, 0, 0, 0}), // flags int32 before collection name
					bytes.NewReader(adminCollectionName),
					bytes.NewReader(
						[]byte{
							0, 0, 0, 0, // numberToSkip int32
							0, 0, 0, 0, // numberToReturn int32
							1, // partial bson document length header
						}),
				),
			},
			Error: "EOF",
		},
		{
			Name:   "error while unmarshaling query document",
			Header: hdr,
			Client: fakeReadWriter{
				Reader: io.MultiReader(
					bytes.NewReader([]byte{0, 0, 0, 0}), // flags int32 before collection name
					bytes.NewReader(adminCollectionName),
					bytes.NewReader(
						[]byte{
							0, 0, 0, 0, // numberToSkip int32
							0, 0, 0, 0, // numberToReturn int32
							5, 0, 0, 0, // bson document length header
							1, // bson document
						}),
				),
			},
			Error: "Document is corrupted",
		},
	}

	for _, c := range cases {
    var lastError LastError
		message := NewProxiedMessage(c.Header, c.Client, nil, &lastError)
		err := p.Proxy(&message)
		if err == nil || !strings.Contains(err.Error(), c.Error) {
			t.Fatalf("did not find expected error for %s, instead found %s", c.Name, err)
		}
	}
}
