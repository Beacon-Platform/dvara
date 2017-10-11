package dvara

import (
	"testing"
	"time"
	"strings"

	"gopkg.in/mgo.v2"

	"github.com/facebookgo/ensure"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/mgotest"
	"github.com/facebookgo/startstop"
	"github.com/facebookgo/stats"
)

type NoopLogger struct {
}

func (l *NoopLogger) Debugf(f string, args ...interface{}) {
}

func (l *NoopLogger) Errorf(f string, args ...interface{}) {
}

type stopper interface {
	Stop()
}

type Harness struct {
	T          testing.TB
	Stopper    stopper // This is either mgotest.Server or mgotest.ReplicaSet
	ReplicaSet *ReplicaSet
	Manager    *StateManager
	Graph      *inject.Graph
	Log        NoopLogger
}

func newHarnessInternal(url string, s stopper, t testing.TB) *Harness {
	replicaSet := ReplicaSet{
		Addrs:                   url,
		ListenAddr:              "",
		PortStart:               2000,
		PortEnd:                 3000,
		MaxConnections:          5,
		MinIdleConnections:      5,
		ServerIdleTimeout:       5 * time.Minute,
		ServerClosePoolSize:     5,
		ClientIdleTimeout:       5 * time.Minute,
		MaxPerClientConnections: 250,
		GetLastErrorTimeout:     5 * time.Minute,
		MessageTimeout:          5 * time.Second,
	}
	manager := NewStateManager(&replicaSet)
	var graph inject.Graph
	err := graph.Provide(
		&inject.Object{Value: &replicaSet},
		&inject.Object{Value: manager},
		&inject.Object{Value: &stats.HookClient{}},
	)
	ensure.Nil(t, err)
	ensure.Nil(t, graph.Populate())
	objects := graph.Objects()
	log := NoopLogger{}
	ensure.Nil(t, startstop.Start(objects, &log))
	return &Harness{
		T:          t,
		Stopper:    s,
		ReplicaSet: &replicaSet,
		Manager:    manager,
		Graph:      &graph,
		Log:        log,
	}
}

type SingleHarness struct {
	*Harness
	MgoServer *mgotest.Server
}

func NewSingleHarness(t testing.TB) *SingleHarness {
	mgoserver := mgotest.NewStartedServer(t)
	return &SingleHarness{
		Harness:   newHarnessInternal(mgoserver.URL(), mgoserver, t),
		MgoServer: mgoserver,
	}
}

type ReplicaSetHarness struct {
	*Harness
	MgoReplicaSet *mgotest.ReplicaSet
}

func NewReplicaSetHarness(n uint, t testing.TB) *ReplicaSetHarness {
	mgoRS := mgotest.NewReplicaSet(n, t)
	return &ReplicaSetHarness{
		Harness:       newHarnessInternal(mgoRS.Addrs()[n-1], mgoRS, t),
		MgoReplicaSet: mgoRS,
	}
}

func (h *Harness) Stop() {
	defer h.Stopper.Stop()
	ensure.Nil(h.T, startstop.Stop(h.Graph.Objects(), &h.Log))
}

func (h *Harness) ProxySession() *mgo.Session {
	return h.Dial(h.Manager.ProxyMembers()[0])
}

func (h *Harness) RealSession() *mgo.Session {
	return h.Dial(h.Manager.currentReplicaSetState.Addrs()[0])
}

// if listenAddr is 0, it may be impossible to connect to it
func listenAddrToConnect(listenAddr string) string {
	if strings.HasPrefix(listenAddr, "[::]") {
		return strings.Replace(listenAddr, "[::]", "127.0.0.1", 1)
	} else {
		return listenAddr
	}
}

func (h *Harness) Dial(u string) *mgo.Session {
	connectAddr := listenAddrToConnect(u)
	session, err := mgo.Dial(connectAddr)
	ensure.Nil(h.T, err, connectAddr)
	session.SetSafe(&mgo.Safe{FSync: true, W: 1})
	session.SetSyncTimeout(time.Minute)
	session.SetSocketTimeout(time.Minute)
	return session
}
