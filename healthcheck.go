package dvara

import (
  "crypto/tls"
	"errors"
	"fmt"
  "net"
	"strings"
	"time"

	"gopkg.in/mgo.v2"

	corelog "github.com/intercom/gocore/log"
)

//HealthChecker -> Run health check to verify is dvara still connected to the replica set
type HealthChecker struct {
	consecutiveFailures        uint
	HealthCheckInterval        time.Duration
	FailedHealthCheckThreshold uint
	Cancel                     bool
	syncTryChan                chan<- struct{}
}

func (checker *HealthChecker) HealthCheck(checkable CheckableMongoConnector, syncTryChan chan<- struct{}) {
	ticker := time.NewTicker(checker.HealthCheckInterval)

	if syncTryChan != nil {
		checker.syncTryChan = syncTryChan
	}

	for {
		select {
		case <-ticker.C:
			checker.tryRunReplicaChecker()
			err := checkable.Check(checker.HealthCheckInterval)
			if err != nil {
				checker.consecutiveFailures++
			} else {
				checker.consecutiveFailures = 0
			}
			if checker.consecutiveFailures >= checker.FailedHealthCheckThreshold {
				checker.consecutiveFailures = 0
				checkable.HandleFailure()
			}
		}
		if checker.Cancel {
			return
		}
	}
}

func (checker *HealthChecker) tryRunReplicaChecker() {
	if checker.syncTryChan != nil {
		select {
		case checker.syncTryChan <- struct{}{}:
		default:
		}
	}
}

type CheckableMongoConnector interface {
	Check(timeout time.Duration) error
	HandleFailure()
}

// Attemps to connect to Mongo through Dvara, with timeout.
func (r *ReplicaSet) Check(timeout time.Duration) error {
	errChan := make(chan error)
	go r.runCheck(errChan)
	// blocking wait
	select {
	case err := <-errChan:
		if err != nil {
			r.Stats.BumpSum("healthcheck.failed", 1)
			corelog.LogErrorMessage(fmt.Sprintf("Failed healthcheck due to %s", err))
		}
		return err
	case <-time.After(timeout):
		r.Stats.BumpSum("healthcheck.failed", 1)
		corelog.LogErrorMessage(fmt.Sprintf("Failed healthcheck due to timeout %s", timeout))
		return errors.New("Failed due to timeout")
	}
}

func (r *ReplicaSet) HandleFailure() {
	corelog.LogErrorMessage("Crashing dvara due to consecutive failed healthchecks")
	r.Stats.BumpSum("healthcheck.failed.panic", 1)
	panic("failed healthchecks")
}

// Attemps to connect to Mongo through Dvara. Blocking call.
func (r *ReplicaSet) runCheck(errChan chan<- error) {
	// dvara opens a port per member of replica set, we don't expect to run more than 5 members in replica set
	addrs := strings.Split(fmt.Sprintf("127.0.0.1:%d,127.0.0.1:%d,127.0.0.1:%d,127.0.0.1:%d,127.0.0.1:%d", r.PortStart, r.PortStart+1, r.PortStart+2, r.PortStart+3, r.PortStart+4), ",")
	err := checkReplSetStatus(addrs, r.Name, r.HealthCheckTLSConfig)
	select {
	case errChan <- err:
	default:
		return
	}
}

func checkReplSetStatus(addrs []string, replicaSetName string, tlsConfig *tls.Config) error {
	info := &mgo.DialInfo{
		Addrs:    addrs,
		FailFast: true,
		// Without direct option, healthcheck fails in case there are only secondaries in the replica set
		Direct:         true,
		ReplicaSetName: replicaSetName,
	}
  if tlsConfig != nil {
    info.DialServer = func(addr* mgo.ServerAddr) (net.Conn, error) {
      return tls.Dial("tcp", addr.String(), tlsConfig)
    }
  }

	session, err := mgo.DialWithInfo(info)
	if err != nil {
		return err
	}
	defer session.Close()
	_, replStatusErr := replSetGetStatus(session)
	return replStatusErr
}
