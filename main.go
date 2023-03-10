package main

import (
	"astra-dialer/astra"
	"log"
	"time"

	"github.com/gocql/gocql"
)

func main() {
	var err error
	astraDatabaseId := ""
	astraToken := ""
	astraHost := ""
	astraKeyspace := ""

	conf := gocql.NewCluster(astraHost)
	conf.DefaultTimestamp = false
	conf.Port = 29042
	conf.PageSize = 500
	conf.Consistency = gocql.LocalQuorum
	conf.ConnectTimeout = 10 * time.Second
	conf.DisableInitialHostLookup = false
	conf.ReconnectInterval = 30 * time.Second
	conf.ReconnectionPolicy = &gocql.ConstantReconnectionPolicy{
		MaxRetries: 3,
		Interval:   1 * time.Second,
	}
	conf.SocketKeepalive = 5 * time.Second
	conf.NumConns = 50
	conf.Timeout = 20 * time.Second
	conf.Keyspace = astraKeyspace
	conf.RetryPolicy = &RetryPolicy{
		NumRetries: 2,
		Min:        100 * time.Millisecond,
		Max:        500 * time.Millisecond,
	}

	conf.HostDialer, err = astra.NewDialerFromURL(astraDatabaseId, astraToken, time.Minute, time.Minute, astraHost)
	if err != nil {
		log.Fatalf("failed to create Astra HostDialer: %v", err)
	}

	conf.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	conf.HostFilter = &astra.HostFilter{}
	conf.DisableInitialHostLookup = false

	conf.Authenticator = gocql.PasswordAuthenticator{
		Username: "token",
		Password: astraToken,
	}

	s, err := conf.CreateSession()
	if err != nil {
		log.Fatalf("failed to create session: %v", err)
	}

	for {
		// do something with this session periodically
		_, err = s.Query("SELECT * FROM system.local").Iter().SliceMap()
		if err != nil {
			log.Fatalf("failed to invoke query: %v", err)
		}

		time.Sleep(time.Second)
	}
}
