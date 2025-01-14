package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitlab.wsq.io/beacon-ext/dvara"
	"github.com/facebookgo/inject"
	"github.com/facebookgo/startstop"
	corelog "github.com/intercom/gocore/log"
)

func main() {
	if err := Main(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func Main() error {
	addrs := flag.String("addrs", "localhost:27017", "comma separated list of mongo addresses")
	clientIdleTimeout := flag.Duration("client_idle_timeout", 60*time.Minute, "idle timeout for client connections")
	getLastErrorTimeout := flag.Duration("get_last_error_timeout", time.Minute, "timeout for getLastError pinning")
	listenAddr := flag.String("listen", "127.0.0.1", "address for listening, for example, 127.0.0.1 for reachable only from the same machine, or 0.0.0.0 for reachable from other machines")
	maxConnections := flag.Uint("max_connections", 100, "maximum number of connections per mongo")
	maxPerClientConnections := flag.Uint("max_per_client_connections", 100, "maximum number of connections from a single client")
	messageTimeout := flag.Duration("message_timeout", 2*time.Minute, "timeout for one message to be proxied")
	password := flag.String("password", "", "mongodb password")
	portEnd := flag.Int("port_end", 6010, "end of port range")
	portStart := flag.Int("port_start", 6000, "start of port range")
	serverClosePoolSize := flag.Uint("server_close_pool_size", 1, "number of goroutines that will handle closing server connections.")
	serverIdleTimeout := flag.Duration("server_idle_timeout", 60*time.Minute, "duration after which a server connection will be considered idle")
	username := flag.String("username", "", "mongo db username")
	metricsAddress := flag.String("metrics", "127.0.0.1:8125", "UDP address to send metrics to datadog, default is 127.0.0.1:8125")
	replicaName := flag.String("replica_name", "", "Replica name, used in metrics and logging, default is empty")
	replicaSetName := flag.String("replica_set_name", "", "Replica set name, used to filter hosts runnning other replica sets")
	healthCheckInterval := flag.Duration("healthcheckinterval", 5*time.Second, "How often to run the health check")
	failedHealthCheckThreshold := flag.Uint("failedhealthcheckthreshold", 3, "How many failed checks before a restart")
	sslPEMKeyFile := flag.String("ssl_pem_key_file", "", "PEM Cert and Private Key file for enabling TLS on the listen sockets")
	mongoSSLPEMKeyFile := flag.String("mongo_ssl_pem_key_file", "", "PEM Cert and private Key file to present to the mongo servers")
	mechanism := flag.String("mechanism", "", "Login mechanism")
	sslSkipVerify := flag.Bool("ssl_skip_verify", false, "Skip SSL hostname verification")
	logQueries := flag.Bool("log_queries", false, "Log all queries")

	flag.Parse()
	statsClient := NewDataDogStatsDClient(*metricsAddress, "replica:"+*replicaName)

	// Actual logger
	corelog.SetupLogFmtLoggerTo(os.Stderr)
	corelog.SetStandardFields("replicaset", *replicaName)
	corelog.UseTimestamp(true)

	cred := dvara.Credential{
		Username:  *username,
		Password:  *password,
		Mechanism: *mechanism,
	}
	sslConfig, sslConfigErr := NewSSLConfig(*sslPEMKeyFile, *mongoSSLPEMKeyFile, *sslSkipVerify, &cred)
	if sslConfigErr != nil {
		return sslConfigErr
	}

	// for the health checks
	var healthCheckTLSConfig *tls.Config
	if sslConfig.tlsConfig != nil {
		// ignore ssl verification for the health checker since we connect via localhost
		healthCheckTLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	replicaSet := dvara.ReplicaSet{
		Addrs:                   *addrs,
		ClientIdleTimeout:       *clientIdleTimeout,
		GetLastErrorTimeout:     *getLastErrorTimeout,
		ListenAddr:              *listenAddr,
		MaxConnections:          *maxConnections,
		MaxPerClientConnections: *maxPerClientConnections,
		MessageTimeout:          *messageTimeout,
		PortEnd:                 *portEnd,
		PortStart:               *portStart,
		ServerClosePoolSize:     *serverClosePoolSize,
		ServerIdleTimeout:       *serverIdleTimeout,
		Cred:                    cred,
		Name:                    *replicaSetName,
		TLSConfig:               sslConfig.tlsConfig,
		BackendTLSConfig:        sslConfig.mongoTLSConfig,
		HealthCheckTLSConfig:    healthCheckTLSConfig,
	}
	stateManager := dvara.NewStateManager(&replicaSet)

	// Log command line args
	startupOptions := []interface{}{}
	flag.CommandLine.VisitAll(func(flag *flag.Flag) {
		if flag.Name != "password" {
			startupOptions = append(startupOptions, flag.Name, flag.Value.String())
		}
	})
	corelog.LogInfoMessage("starting with command line arguments", startupOptions...)

	// configure extensions
	var extensions = []dvara.ProxyExtension{}
	if *logQueries {
		extensions = append(extensions, &dvara.QueryLogger{})
	}
	extensionStackInstance := dvara.NewProxyExtensionStack(extensions)

	// Wrapper for inject
	log := Logger{}

	var graph inject.Graph
	err := graph.Provide(
		&inject.Object{Value: &replicaSet},
		&inject.Object{Value: &statsClient},
		&inject.Object{Value: &extensionStackInstance},
		&inject.Object{Value: stateManager},
	)
	if err != nil {
		return err
	}
	if err := graph.Populate(); err != nil {
		return err
	}
	objects := graph.Objects()

	hc := &dvara.HealthChecker{
		HealthCheckInterval:        *healthCheckInterval,
		FailedHealthCheckThreshold: *failedHealthCheckThreshold,
	}

	if err := startstop.Start(objects, &log); err != nil {
		return err
	}
	defer startstop.Stop(objects, &log)

	syncChan := make(chan struct{})
	go stateManager.KeepSynchronized(syncChan)
	go hc.HealthCheck(&replicaSet, syncChan)

	ch := make(chan os.Signal, 2)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
	<-ch
	signal.Stop(ch)
	return nil
}
