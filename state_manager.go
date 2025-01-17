package dvara

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/facebookgo/stackerr"
	corelog "github.com/intercom/gocore/log"
	"time"
)

type StateManager struct {
	*sync.RWMutex
	replicaSet             *ReplicaSet
	baseAddrs              string
	currentReplicaSetState *ReplicaSetState
	syncTryChan            chan struct{}

	proxyToReal map[string]string
	realToProxy map[string]string
	proxies     map[string]*Proxy
	refreshTime time.Time

	ExtensionStack *ProxyExtensionStack `inject:""`
}

func NewStateManager(replicaSet *ReplicaSet) *StateManager {
	manager := &StateManager{
		RWMutex:     &sync.RWMutex{},
		replicaSet:  replicaSet,
		baseAddrs:   replicaSet.Addrs,
		proxyToReal: make(map[string]string),
		realToProxy: make(map[string]string),
		proxies:     make(map[string]*Proxy),
	}
	return manager
}

func (manager *StateManager) Start() error {
	corelog.LogInfoMessage("starting manager")
	manager.Lock()
	defer manager.Unlock()
	var err error
	manager.currentReplicaSetState, err = manager.generateReplicaSetState()
	if err != nil {
		return err
		return errors.New(fmt.Sprintf("error starting statemanager, replicaset in flux: %v", err))
	}
	healthyAddrs := manager.currentReplicaSetState.Addrs()

	// Ensure we have at least one health address.
	if len(healthyAddrs) == 0 {
		return stackerr.Newf("no healthy primaries or secondaries: %s", manager.replicaSet.Addrs)
	}

	manager.addProxies(healthyAddrs...)

	for _, proxy := range manager.proxies {
		go manager.startProxy(proxy)
	}
	manager.refreshTime = time.Now()
	return nil
}

func (manager *StateManager) KeepSynchronized(syncChan chan struct{}) {
	manager.syncTryChan = syncChan
	for {
		select {
		case <-manager.syncTryChan:
			manager.Synchronize()
		}
	}
}

// Get new state for a replica set, and synchronize internal state.
func (manager *StateManager) Synchronize() {
	defer manager.replicaSet.Stats.BumpTime("replica.manager.time").End()
	manager.replicaSet.Stats.BumpHistogram("replica.manager.rs_state_age", float64(time.Since(manager.refreshTime).Nanoseconds()))

	manager.RLock()
	newState, err := manager.generateReplicaSetState()
	if err != nil {
		manager.replicaSet.Stats.BumpSum("replica.manager.failed_state_check", 1)
		corelog.LogErrorMessage(fmt.Sprintf("all nodes possibly down?: %s", err))
		manager.RUnlock()
		return
	}

	comparison, err := manager.getComparison(manager.currentReplicaSetState.lastRS, newState.lastRS)
	if err != nil {
		manager.replicaSet.Stats.BumpSum("replica.manager.failed_comparison", 1)
		corelog.LogErrorMessage(fmt.Sprintf("Manager failed comparison %s", err))
		manager.RUnlock()
		return
	}
	manager.RUnlock() // all reads done

	defer manager.replicaSet.Stats.BumpTime("replica.manager.time.locked").End()

	manager.Lock()
	defer manager.Unlock()
	if err = manager.addRemoveProxies(comparison); err != nil {
		manager.replicaSet.Stats.BumpSum("replica.manager.failed_proxy_update", 1)
		corelog.LogErrorMessage(fmt.Sprintf("Manager failed proxy update %s", err))
		return
	}

	manager.stopStartProxies(comparison)
	manager.currentReplicaSetState = newState

	// Add discovered nodes to seed address list. Over time if the original seed
	// nodes have gone away and new nodes have joined this ensures that we'll
	// still be able to connect.
	rawAddrs := strings.Split(manager.baseAddrs, ",")
	manager.baseAddrs = strings.Join(uniq(append(rawAddrs, manager.currentReplicaSetState.Addrs()...)), ",")
	manager.refreshTime = time.Now()
}

func (manager *StateManager) ProxyMembers() []string {
	manager.RLock()
	defer manager.RUnlock()
	members := make([]string, 0, len(manager.proxyToReal))
	for r := range manager.proxyToReal {
		members = append(members, r)
	}
	return members
}

// implement ProxyMapper interface
func (manager *StateManager) Proxy(h string) (string, error) {
	manager.RLock()
	defer manager.RUnlock()
	p, ok := manager.realToProxy[h]
	if !ok {
		return "", fmt.Errorf("mongo %s is not in ReplicaSet", h)
	}
	return p, nil
}

// add new proxies
func (manager *StateManager) addProxies(addresses ...string) error {
	proxies, err := manager.generateProxies(addresses...)
	if err != nil {
		return err
	}
	for _, proxy := range proxies {
		if _, err := manager.addProxy(proxy); err != nil {
			return err
		}
	}
	return nil
}

func (manager *StateManager) removeProxies(proxies ...*Proxy) error {
	for _, proxy := range proxies {
		manager.removeProxy(proxy)
	}
	return nil
}

func (manager *StateManager) generateProxies(addresses ...string) ([]*Proxy, error) {
	proxies := []*Proxy{}
	for _, address := range addresses {
		listener, err := manager.replicaSet.newListener()
		if err != nil {
			return nil, err
		}

		p := &Proxy{
			ReplicaSet:     manager.replicaSet,
			ClientListener: listener,
			ProxyAddr:      manager.replicaSet.proxyAddr(listener),
			Cred:           manager.replicaSet.Cred,
			MongoAddr:      address,
			extensions:     manager.ExtensionStack.GetExtensions(),
			TLSConfig:      manager.replicaSet.BackendTLSConfig,
		}

		proxies = append(proxies, p)
	}
	return proxies, nil
}

func (manager *StateManager) generateReplicaSetState() (*ReplicaSetState, error) {
	replicaSet := manager.replicaSet
	addrs := strings.Split(manager.baseAddrs, ",")
	return replicaSet.ReplicaSetStateCreator.FromAddrs(replicaSet.Cred, addrs, replicaSet.Name, replicaSet.BackendTLSConfig)
}

func (manager *StateManager) getComparison(oldResp, newResp *replSetGetStatusResponse) (*ReplicaSetComparison, error) {
	comparison := &ReplicaSetComparison{
		ExtraMembers:   make(map[string]*Proxy),
		MissingMembers: make(map[string]*Proxy),
	}

	if (oldResp == nil || len(oldResp.Members) == 0) && (newResp == nil || len(newResp.Members) == 0) {
		return nil, errors.New("No members found")
	}
	if oldResp == nil || newResp == nil {
		return nil, errors.New("No members found")
	}

	for _, m := range oldResp.Members {
		if proxy, ok := manager.findProxyForMember(m); ok {
			comparison.ExtraMembers[m.Name] = proxy
		}
	}

	for _, m := range newResp.Members {
		if _, ok := comparison.ExtraMembers[m.Name]; ok {
			// if we've got the same thing in new then it's not extra
			delete(comparison.ExtraMembers, m.Name)
		} else {
			// otherwise it's missing
			comparison.MissingMembers[m.Name] = nil // we don't have a proxy to add just yet
		}
	}
	return comparison, nil
}

func (manager *StateManager) addRemoveProxies(comparison *ReplicaSetComparison) error {
	for _, proxy := range comparison.ExtraMembers {
		manager.removeProxy(proxy)
	}

	for name, _ := range comparison.MissingMembers {
		proxies, err := manager.generateProxies(name)
		if err != nil {
			return err
		}
		p, err := manager.addProxy(proxies[0])
		if err != nil {
			return err
		}
		comparison.MissingMembers[name] = p
	}
	return nil
}

func (manager *StateManager) addProxy(proxy *Proxy) (*Proxy, error) {
	if _, ok := manager.proxyToReal[proxy.ProxyAddr]; ok {
		return nil, fmt.Errorf("proxy %s already used in ReplicaSet", proxy.ProxyAddr)
	}
	if _, ok := manager.realToProxy[proxy.MongoAddr]; ok {
		return nil, fmt.Errorf("mongo %s already exists in ReplicaSet", proxy.MongoAddr)
	}
	corelog.LogInfoMessage(fmt.Sprintf("added %s", proxy))
	manager.proxyToReal[proxy.ProxyAddr] = proxy.MongoAddr
	manager.realToProxy[proxy.MongoAddr] = proxy.ProxyAddr
	manager.proxies[proxy.ProxyAddr] = proxy
	return proxy, nil
}

func (manager *StateManager) removeProxy(proxy *Proxy) {
	if _, ok := manager.proxyToReal[proxy.ProxyAddr]; !ok {
		corelog.LogErrorMessage(fmt.Sprintf("proxy %s does not exist in ReplicaSet", proxy.ProxyAddr))
	}
	if _, ok := manager.realToProxy[proxy.MongoAddr]; !ok {
		corelog.LogErrorMessage(fmt.Sprintf("mongo %s does not exist in ReplicaSet", proxy.ProxyAddr))
	}
	corelog.LogInfoMessage(fmt.Sprintf("removed %s", proxy))
	delete(manager.proxyToReal, proxy.ProxyAddr)
	delete(manager.realToProxy, proxy.MongoAddr)
	delete(manager.proxies, proxy.ProxyAddr)
}

func (manager *StateManager) stopStartProxies(comparison *ReplicaSetComparison) {
	t := manager.replicaSet.Stats.BumpTime("replica.manager.start_stop_proxies.time")
	defer t.End()
	for _, proxy := range comparison.ExtraMembers {
		go manager.stopProxy(proxy)
	}

	for _, proxy := range comparison.MissingMembers {
		go manager.startProxy(proxy)
	}
}

func (manager *StateManager) startProxy(proxy *Proxy) {
	if err := proxy.Start(); err != nil {
		corelog.LogErrorMessage(fmt.Sprintf("Failed to start proxy %s", proxy))
	}
}

func (manager *StateManager) stopProxy(proxy *Proxy) {
	if err := proxy.stop(true); err != nil {
		corelog.LogErrorMessage(fmt.Sprintf("Failed to stop proxy %s", proxy))
	}
}

func (manager *StateManager) findProxyForMember(member statusMember) (*Proxy, bool) {
	proxyName, ok := manager.realToProxy[member.Name]
	if !ok {
		return nil, false
	}
	proxy, ok := manager.proxies[proxyName]
	return proxy, ok
}
