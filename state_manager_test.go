package dvara

import (
	"fmt"
	"sync"
	"testing"
)

func TestManagerFindsMissingExtraMembers(t *testing.T) {
	manager := newManager()
	manager.addProxies("a", "b")

	comparison, _ := manager.getComparison(getStatusResponse("a", "b"), getStatusResponse("a", "c"))
	if _, ok := comparison.ExtraMembers["b"]; !ok {
		t.Fatal("Extra member b not found")
	}
	if _, ok := comparison.MissingMembers["c"]; !ok {
		t.Fatal("Missing member c not found")
	}
	if len(comparison.ExtraMembers) != 1 {
		t.Fatalf("too many extra members, expecting %d, got %d", 1, len(comparison.ExtraMembers))
	}
	if len(comparison.MissingMembers) != 1 {
		t.Fatalf("too many extra members, expecting %d, got %d", 1, len(comparison.MissingMembers))
	}
}

func TestManagerAddsAndRemovesProxies(t *testing.T) {
	manager := newManager()
	manager.addProxies("mongoA", "mongoB")
	comparison, _ := manager.getComparison(getStatusResponse("mongoA", "mongoB"), getStatusResponse("mongoA", "mongoC"))
	manager.addRemoveProxies(comparison)
	if _, ok := manager.proxies[manager.realToProxy["mongoA"]]; !ok {
		t.Fatal("proxyA was removed")
	}
	if _, ok := manager.proxies[manager.realToProxy["mongoB"]]; ok {
		t.Fatal("proxyB was not removed")
	}
	if _, ok := manager.proxies[manager.realToProxy["mongoC"]]; !ok {
		t.Fatal("proxyC was not added")
	}
}

func TestManagerWithMissingMembersInCurrentState(t *testing.T) {
	manager := newManager()
	manager.addProxies("mongoA")
	comparison, _ := manager.getComparison(getStatusResponse("mongoA", "mongoB"), getStatusResponse("mongoA", "mongoC"))
	if _, ok := comparison.ExtraMembers["mongoB"]; ok {
		t.Fatal("mongoB is not currently running")
	}
}

func TestReplicaSetMembers(t *testing.T) {
	t.Parallel()
	h := NewReplicaSetHarness(3, t)
	defer h.Stop()

	proxyMembers := h.Manager.ProxyMembers()
	session := h.ProxySession()
	defer session.Close()
	status, err := replSetGetStatus(session)
	if err != nil {
		t.Fatal(err)
	}

outerProxyResponseCheckLoop:
	for _, m := range status.Members {
		for _, p := range proxyMembers {
			if m.Name == p {
				continue outerProxyResponseCheckLoop
			}
		}
		t.Fatalf("Unexpected member: %s", m.Name)
	}
}

func TestProxyNotInReplicaSet(t *testing.T) {
	t.Parallel()
	h := NewReplicaSetHarness(3, t)
	defer h.Stop()
	addr := "127.0.0.1:666"
	expected := fmt.Sprintf("mongo %s is not in ReplicaSet", addr)
	_, err := h.Manager.Proxy(addr)
	if err == nil || err.Error() != expected {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func TestAddSameProxyToReplicaSet(t *testing.T) {
	t.Parallel()
	m := newManager()
	p := &Proxy{
		ProxyAddr: "1",
		MongoAddr: "2",
	}
	if _, err := m.addProxy(p); err != nil {
		t.Fatal(err)
	}
	expected := fmt.Sprintf("proxy %s already used in ReplicaSet", p.ProxyAddr)
	_, err := m.addProxy(p)
	if err == nil || err.Error() != expected {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func TestAddSameMongoToReplicaSet(t *testing.T) {
	t.Parallel()
	m := newManager()
	p := &Proxy{
		ProxyAddr: "1",
		MongoAddr: "2",
	}
	if _, err := m.addProxy(p); err != nil {
		t.Fatal(err)
	}
	p = &Proxy{
		ProxyAddr: "3",
		MongoAddr: p.MongoAddr,
	}
	expected := fmt.Sprintf("mongo %s already exists in ReplicaSet", p.MongoAddr)
	_, err := m.addProxy(p)
	if err == nil || err.Error() != expected {
		t.Fatalf("did not get expected error, got: %s", err)
	}
}

func TestAddRemoveProxy(t *testing.T) {
	t.Parallel()
	m := newManager()
	p := &Proxy{
		ProxyAddr: "1",
		MongoAddr: "2",
	}
	if _, err := m.addProxy(p); err != nil {
		t.Fatal(err)
	}
	m.removeProxy(p)
	if _, ok := m.proxies[p.ProxyAddr]; ok {
		t.Fatal("failed to remove proxy")
	}
}

func newManager() *StateManager {
	replicaSet := setupReplicaSet()
	return newManagerWithReplicaSet(replicaSet)
}

func newManagerWithReplicaSet(replicaSet *ReplicaSet) *StateManager {
	extensionStack := NewProxyExtensionStack([]ProxyExtension{})
	return &StateManager{
		RWMutex:        &sync.RWMutex{},
		replicaSet:     replicaSet,
		baseAddrs:      replicaSet.Addrs,
		proxyToReal:    make(map[string]string),
		realToProxy:    make(map[string]string),
		proxies:        make(map[string]*Proxy),
		ExtensionStack: &extensionStack,
	}
}

func getStatusResponse(memberNames ...string) *replSetGetStatusResponse {
	members := []statusMember{}
	for _, name := range memberNames {
		members = append(members, statusMember{Name: name, State: "state"})
	}
	return &replSetGetStatusResponse{
		Members: members,
	}
}
