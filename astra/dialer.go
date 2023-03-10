package astra

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/datastax/astra-client-go/v2/astra"
	proxy "github.com/datastax/cql-proxy/astra"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/raver119/retry"
)

const AstraApiEndpoint = "https://api.astra.datastax.com"

type dialer struct {
	// endpoint is the latest metadata from AstraDB that includes SNI gateway address and contact points
	endoint           *atomic.Pointer[Endpoint]
	contactPointIndex int32

	// bundle is a bundle of AstraDB metadata, including TLS certificates.
	bundle *proxy.Bundle

	// used only to fetch the metadata from AstraDB
	bundleHost string

	// used to establish connections
	dialer net.Dialer

	// timeout to wait for the metadata
	timeout time.Duration
}

// NewDialerFromBundle function creates a new AstraDB-aware gocql.HostDialer with a given given AstraDB bundle available locally.
func NewDialerFromBundle(path string, metaTimeout, refreshTimeout time.Duration, gatewayHost string) (gocql.HostDialer, error) {
	bundle, err := proxy.LoadBundleZipFromPath(path)
	if err != nil {
		return nil, err
	}

	return initializeDialer(bundle, metaTimeout, refreshTimeout, gatewayHost)
}

// NewDialerFromURL function creates a new AstraDB-aware gocql.HostDialer for a given databaseID via AstraDB API.
func NewDialerFromURL(databaseID, token string, metaTimeout, refreshTimeout time.Duration, gatewayHost string) (gocql.HostDialer, error) {
	// invoke the same code from the astra early in order to avoid the panic if the connection fails later.
	ctx, cancel := context.WithTimeout(context.Background(), metaTimeout)
	defer cancel()
	{
		client, err := astra.NewClientWithResponses(AstraApiEndpoint, func(c *astra.Client) error {
			c.RequestEditors = append(c.RequestEditors, func(ctx context.Context, req *http.Request) error {
				req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
				return nil
			})
			return nil
		})
		if err != nil {
			return nil, errors.Wrapf(err, "unable to create AstraDB client")
		}

		// this request in cql-proxy code is used without checking the error, so we check it here once, on start.
		// see this: https://github.com/datastax/cql-proxy/issues/109
		_, err = client.GenerateSecureBundleURLWithResponse(ctx, astra.DatabaseIdParam(databaseID))
		if err != nil {
			return nil, errors.Wrapf(err, "unable to connect to AstraDB API.")
		}
	}

	bundle, err := proxy.LoadBundleZipFromURL(AstraApiEndpoint, databaseID, token, metaTimeout)
	if err != nil {
		return nil, err
	}

	return initializeDialer(bundle, metaTimeout, refreshTimeout, gatewayHost)
}

// initializeDialer function creates a new AstraDB-aware gocql.HostDialer with a given given AstraDB bundle.
func initializeDialer(bundle *proxy.Bundle, metaTimeout, refreshPeriod time.Duration, gatewayHost string) (hd *dialer, err error) {
	hd = &dialer{
		bundle:     bundle,
		bundleHost: gatewayHost,
		timeout:    metaTimeout,
		endoint:    &atomic.Pointer[Endpoint]{},
	}

	// initial refresh of the metadata
	err = hd.refreshMetadata(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "unable to fetch initial metadata")
	}

	ep := hd.endoint.Load()
	if ep == nil {
		return nil, errors.New("unable to fetch initial metadata")
	}

	log.Printf("Region: %s; SNI gateway address: %s; ContactPoints: %v", ep.Region, ep.SniAddress, ep.ContactPoints)

	// we need to keep AstraDB metadata up to date. we'll refresh it periodically, until the app terminated
	go hd.backgroundRefresher(context.Background(), refreshPeriod)

	return hd, nil
}

// DialHost function is called by gocql to establish a connection to a Cassandra node.
// This implementation adds support for AstraDB by using the SNI address/nodes and the TLS certificate bundle.
func (d *dialer) DialHost(ctx context.Context, host *gocql.HostInfo) (*gocql.DialedHost, error) {
	sniAddr, contactPoints, err := d.resolveMetadata(ctx, d.bundleHost)
	if err != nil {
		return nil, errors.Wrap(err, "unable to resolve metadata")
	}

	addr, err := lookupHost(sniAddr)
	if err != nil {
		return nil, errors.Wrap(err, "unable to resolve SNI address")
	}

	conn, err := d.dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "unable to connect to SNI proxy")
	}

	hostId := host.HostID()
	if hostId == "" {
		hostId = contactPoints[int(atomic.AddInt32(&d.contactPointIndex, 1))%len(contactPoints)]
	}

	tlsConn := tls.Client(conn, copyTLSConfig(d.bundle, hostId))
	if err = tlsConn.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, errors.Wrap(err, "unable to perform TLS handshake")
	}

	return &gocql.DialedHost{
		Conn:            tlsConn,
		DisableCoalesce: true, // See https://github.com/mpenick/gocqlastra/issues/1
	}, nil
}

// refreshMetadata fetches the latest metadata from AstraDB and updates the dialer
func (d *dialer) refreshMetadata(ctx context.Context) error {
	endpoint, err := d.fetchMetadata(ctx)
	if err != nil {
		log.Printf("ERROR: unable to refresh metadata: %v", err)
		return err
	}

	d.endoint.Store(endpoint)
	return nil
}

// backgroundRefresher function is a loop that periodically refreshes the metadata
func (d *dialer) backgroundRefresher(ctx context.Context, period time.Duration) error {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			// error is ignored, we'll be trying again
			retry.UntilSucceededOrCancelledWithDelay(ctx, 10*time.Second, func() error {
				return d.refreshMetadata(ctx)
			})
		}
	}
}

// fetchMetadata fetches the latest metadata from AstraDB: SNI address and contact points (nodes within the AstraDB cluster)
func (d *dialer) fetchMetadata(ctx context.Context) (*Endpoint, error) {
	var metadata *astraMetadata

	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	httpsClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: d.bundle.TLSConfig.Clone(),
		},
	}

	// Sending request to the AstraDB API endpoint to fetch the metadata.
	url := fmt.Sprintf("https://%s:%d/metadata", d.bundleHost, d.bundle.Port)
	req, err := http.NewRequestWithContext(ctx, "GET", url, http.NoBody)
	if err != nil {
		return nil, errors.Wrap(err, "unable to create request")
	}

	response, err := httpsClient.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get Astra metadata from %s", url)
	}

	body, err := readAllWithTimeout(ctx, response.Body)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read response body")
	}

	if err = json.Unmarshal(body, &metadata); err != nil {
		return nil, err
	}

	var endpoint Endpoint
	endpoint.SniAddress = metadata.ContactInfo.SniProxyAddress
	endpoint.ContactPoints = metadata.ContactInfo.ContactPoints
	endpoint.Region = metadata.Region

	return &endpoint, err
}

// resolveMetadata returns the latest metadata for the AstraDB.
// If the metadata is not available, it will try to fetch/update it via the AstraDB API.
func (d *dialer) resolveMetadata(ctx context.Context, bundleHost string) (string, []string, error) {
	if endpoint := d.endoint.Load(); endpoint != nil {
		return endpoint.SniAddress, endpoint.ContactPoints, nil
	}

	// if we don't have metadata, refresh it so it'll be fetched
	endpoint, err := d.fetchMetadata(ctx)
	if err != nil {
		return "", nil, errors.Wrap(err, "unable to fetch metadata")
	}

	// in worst case we'll replace the existing metadata with the new one
	d.endoint.Store(endpoint)

	// if refresh failed, return the error
	return endpoint.SniAddress, endpoint.ContactPoints, err
}

// copyTLSConfig creates a TLS config for the connection, based on the TLS config provided via the bundle.
// Copy gets the remote server name defined as one of the contact points.
func copyTLSConfig(bundle *proxy.Bundle, serverName string) *tls.Config {
	tlsConfig := bundle.TLSConfig.Clone()
	tlsConfig.ServerName = serverName
	tlsConfig.InsecureSkipVerify = true // unavoidable in this case
	tlsConfig.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) (err error) {
		if len(rawCerts) == 0 {
			return errors.New("no raw certs provided")
		}

		certs := make([]*x509.Certificate, len(rawCerts))
		for i, asn1Data := range rawCerts {
			cert, err := x509.ParseCertificate(asn1Data)
			if err != nil {
				return errors.Wrapf(err, "tls: failed to parse certificate from server")
			}
			certs[i] = cert
		}

		opts := x509.VerifyOptions{
			Roots:         tlsConfig.RootCAs,
			CurrentTime:   time.Now(),
			DNSName:       bundle.Host,
			Intermediates: x509.NewCertPool(),
		}

		// if we have more than 1 certificates in the chain, we treat them as intermediate certificates.
		if len(certs) > 1 {
			for _, cert := range certs[1:] {
				opts.Intermediates.AddCert(cert)
			}
		}

		verifiedChains, err = certs[0].Verify(opts)
		return err
	}
	return tlsConfig
}

// readAllWithTimeout reads all data from the reader (via io.ReadAll) with a timeout.
func readAllWithTimeout(ctx context.Context, r io.Reader) (bytes []byte, err error) {
	ch := make(chan struct{})

	go func() {
		bytes, err = io.ReadAll(r)
		close(ch)
	}()

	select {
	case <-ch:
	case <-ctx.Done():
		return nil, errors.New("timeout reading data")
	}

	return bytes, err
}

// lookupHost function is a wrapper around net.LookupHost.
func lookupHost(hostWithPort string) (string, error) {
	host, port, err := net.SplitHostPort(hostWithPort)
	if err != nil {
		return "", err
	}
	addrs, err := net.LookupHost(host)
	if err != nil {
		return "", err
	}
	addr := addrs[rand.Intn(len(addrs))]
	if len(port) > 0 {
		addr = net.JoinHostPort(addr, port)
	}
	return addr, nil
}

type contactInfo struct {
	TypeName        string   `json:"type"`
	LocalDc         string   `json:"local_dc"`
	SniProxyAddress string   `json:"sni_proxy_address"`
	ContactPoints   []string `json:"contact_points"`
}

type astraMetadata struct {
	Version     int         `json:"version"`
	Region      string      `json:"region"`
	ContactInfo contactInfo `json:"contact_info"`
}
