package dvara

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sort"
	"time"

	"github.com/davecgh/go-spew/spew"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	corelog "github.com/intercom/gocore/log"
)

const errNotReplSet = "not running with --replSet"

const (
  RS_PRIMARY = 1
  RS_SECONDARY = 2
  RS_ARBITER = 7
  RS_DOWN = 8
)

var errNoReachableServers = errors.New("no reachable servers")
var errRemovedReplica = errors.New("removed replica still present")

// ReplicaSetState is a snapshot of the RS configuration at some point in time.
type ReplicaSetState struct {
	lastRS *replSetGetStatusResponse
	lastIM *isMasterResponse
}

// NewReplicaSetState creates a new ReplicaSetState using the given address.
func NewReplicaSetState(cred Credential, addr string, tlsConfig *tls.Config) (*ReplicaSetState, error) {
	const TIMEOUT = 500 * time.Millisecond

	mechanism := cred.Mechanism
	source := cred.Source
	if mechanism == "MONGODB-X509" {
		source = "$external"
	} else if source == "" {
		source = "admin"
	}

	info := &mgo.DialInfo{
		Addrs:     []string{addr},
		Username:  cred.Username,
		Password:  cred.Password,
		Source:    source,
		Mechanism: mechanism,
		Direct:    true,
		FailFast:  true,
		Timeout:   TIMEOUT,
	}
	if tlsConfig != nil {
		info.DialServer = func(addr *mgo.ServerAddr) (net.Conn, error) {
			return tls.Dial("tcp", addr.String(), tlsConfig)
		}
	}
	session, err := mgo.DialWithInfo(info)
	if err != nil {
		return nil, errNoReachableServers
	}
	session.SetMode(mgo.Monotonic, true)
	session.SetSyncTimeout(TIMEOUT)
	session.SetSocketTimeout(TIMEOUT)
	defer session.Close()

	var r ReplicaSetState
	if r.lastRS, err = filterReplGetStatus(replSetGetStatus(session)); err != nil {
		return nil, err
	}

	if r.lastIM, err = isMaster(session); err != nil {
		return nil, err
	}

	if r.lastRS != nil && len(r.lastRS.Members) == 1 {
		n := r.lastRS.Members[0]
		if n.State == ReplicaStateRemoved {
			return nil, errRemovedReplica
		}
		if n.State != ReplicaStatePrimary && n.State != ReplicaStateSecondary {
			return nil, fmt.Errorf("single node RS in bad state: %s", spew.Sdump(r))
		}
	}

	// nodes starting up are invalid
	if r.lastRS != nil {
		for _, member := range r.lastRS.Members {
			if member.Self && (member.State == ReplicaStateStartup || member.State == ReplicaStateStartup2 || member.State == ReplicaStateUnknown) {
				return nil, fmt.Errorf("node is busy starting up: %s", member.Name)
			}
		}
	}

	return &r, nil
}

// AssertEqual checks if the given ReplicaSetState equals this one. It returns
// a rich error message including the entire state for easier debugging.
func (r *ReplicaSetState) AssertEqual(o *ReplicaSetState) error {
	if r.Equal(o) {
		return nil
	}
	return fmt.Errorf(
		"conflicting ReplicaSetState:\n%s\nVS\n%s",
		spew.Sdump(r),
		spew.Sdump(o),
	)
}

// Equal returns true if the given ReplicaSetState is the same as this one.
func (r *ReplicaSetState) Equal(o *ReplicaSetState) bool {
	return r.SameIM(o.lastIM) && r.SameRS(o.lastRS)
}

// SameRS checks if the given replSetGetStatusResponse is the same as the one
// we have.
func (r *ReplicaSetState) SameRS(o *replSetGetStatusResponse) bool {
	res := sameRSMembers(r.lastRS, o)
  return res
}

// SameIM checks if the given isMasterResponse is the same as the one we have.
func (r *ReplicaSetState) SameIM(o *isMasterResponse) bool {
	res := sameIMMembers(r.lastIM, o)
  return res
}

// Addrs returns the addresses of members in primary or secondary state.
func (r *ReplicaSetState) Addrs() []string {
	var members []string
	for _, m := range r.lastRS.Members {
		if m.State == ReplicaStatePrimary || m.State == ReplicaStateSecondary {
			members = append(members, m.Name)
		}
	}
	return members
}

// ReplicaSetStateCreator allows for creating a ReplicaSetState from a given
// set of seed addresses.
type ReplicaSetStateCreator struct {
}

// FromAddrs creates a ReplicaSetState from the given set of see addresses. It
// requires the addresses to be part of the same Replica Set.
func (c *ReplicaSetStateCreator) FromAddrs(cred Credential, addrs []string, replicaSetName string, tlsConfig *tls.Config) (*ReplicaSetState, error) {
	var r *ReplicaSetState
	for _, addr := range addrs {
		ar, err := NewReplicaSetState(cred, addr, tlsConfig)
		if err != nil {
			if err != errNoReachableServers {
				corelog.LogErrorMessage(fmt.Sprintf("ignoring failure against address %s: %s", addr, err))
			}
			continue
		}

		if replicaSetName != "" {
			if ar.lastRS == nil {
				corelog.LogErrorMessage(fmt.Sprintf("ignoring standalone node %q not in expected replset: %q", addr, replicaSetName))
				continue
			}
			if ar.lastRS.Name != replicaSetName {
				corelog.LogErrorMessage(fmt.Sprintf("ignoring node %q not in expected replset: %q vs %q", addr, ar.lastRS.Name, replicaSetName))
				continue
			}
		}

		// First successful address.
		if r == nil {
			r = ar
			continue
		}

		// Ensure same as already established ReplicaSetState.
		if err := r.AssertEqual(ar); err != nil {
			return nil, err
		}
	}

	if r == nil {
		return nil, fmt.Errorf("could not connect to any provided addresses: %v", addrs)
	}

	return r, nil
}

var (
	replSetGetStatusQuery = bson.D{
		bson.DocElem{Name: "replSetGetStatus", Value: 1},
	}
	isMasterQuery = bson.D{
		bson.DocElem{Name: "isMaster", Value: 1},
	}
)

func filterReplGetStatus(r *replSetGetStatusResponse, e error) (*replSetGetStatusResponse, error) {
	var validMembers []statusMember
	if r != nil {
		for _, element := range r.Members {
      switch element.StateCode {
      case RS_PRIMARY, RS_ARBITER, RS_SECONDARY, RS_DOWN:
        validMembers = append(validMembers, element)
      }
    }
		r.Members = validMembers
	}

	return r, e
}

func replSetGetStatus(s *mgo.Session) (*replSetGetStatusResponse, error) {
	var res replSetGetStatusResponse
	if err := s.Run(replSetGetStatusQuery, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

func isMaster(s *mgo.Session) (*isMasterResponse, error) {
	var res isMasterResponse
	if err := s.Run(isMasterQuery, &res); err != nil {
		return nil, fmt.Errorf("error in isMaster: %s", err)
	}
	return &res, nil
}

func sameRSMembers(a *replSetGetStatusResponse, b *replSetGetStatusResponse) bool {
	if (a == nil || len(a.Members) == 0) && (b == nil || len(b.Members) == 0) {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	l := len(a.Members)
	if l != len(b.Members) {
		return false
	}
	aMembers := make([]string, 0, l)
	bMembers := make([]string, 0, l)
	for i := 0; i < l; i++ {
		aM := a.Members[i]
		aMembers = append(aMembers, aM.Name)
		bM := b.Members[i]
		bMembers = append(bMembers, bM.Name)
	}
	sort.Strings(aMembers)
	sort.Strings(bMembers)
	for i := 0; i < l; i++ {
		if aMembers[i] != bMembers[i] {
			return false
		}
	}
	return true
}

var emptyIsMasterResponse = isMasterResponse{}

func sameIMMembers(a *isMasterResponse, b *isMasterResponse) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil {
		a = &emptyIsMasterResponse
	}
	if b == nil {
		b = &emptyIsMasterResponse
	}
	l := len(a.Hosts)
	if l != len(b.Hosts) {
		return false
	}
	aHosts := make([]string, 0, l+1)
	bHosts := make([]string, 0, l+1)
	for i := 0; i < l; i++ {
		aHosts = append(aHosts, a.Hosts[i])
		bHosts = append(bHosts, b.Hosts[i])
	}
	sort.Strings(aHosts)
	sort.Strings(bHosts)
  // the primary may not be reachable on a member so compare them only if both are non-empty
  if len(a.Primary) > 0 && len(b.Primary) > 0 {
    aHosts = append(aHosts, a.Primary)
    bHosts = append(bHosts, b.Primary)
  }
	for i := range aHosts {
		if aHosts[i] != bHosts[i] {
			return false
		}
	}
	return true
}
