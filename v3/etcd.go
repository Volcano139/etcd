package etcd

import (
	"context"
	"crypto/tls"
	"errors"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"

	"go.etcd.io/etcd/pkg/transport"
)

var (
	// ErrNoKey indicates a client method needs a key but receives none.
	ErrNoKey = errors.New("no key provided")

	// ErrNoValue indicates a client method needs a value but receives none.
	ErrNoValue = errors.New("no value provided")
)

// Client is a wrapper around the etcd client.
type Client interface {
	GetEntries(prefix string) (*clientv3.GetResponse, error)

	WatchPrefix(prefix string, ch chan clientv3.WatchResponse)

	// Register a service with etcd.
	Register(is Service) (int64, error)

	// Deregister a service with etcd.
	Deregister(id int64, s Service) error

	// LeaseID returns the lease id created for this service instance
	LeaseID(id int64) int64
}

type register struct {
	leaseID clientv3.LeaseID

	hbch <-chan *clientv3.LeaseKeepAliveResponse
	// Lease interface instance, used to leverage Lease.Close()
	leaser clientv3.Lease
}

type client struct {
	cli *clientv3.Client
	ctx context.Context

	kv clientv3.KV

	// Watcher interface instance, used to leverage Watcher.Close()
	watcher clientv3.Watcher
	// watcher context
	wctx context.Context
	// watcher cancel func
	wcf context.CancelFunc

	ch chan DicoveryData

	gid string

	leaseId clientv3.LeaseID

	lock sync.Mutex
}

// ClientOptions defines options for the etcd client. All values are optional.
// If any duration is not specified, a default of 3 seconds will be used.
type ClientOptions struct {
	Cert          string
	Key           string
	CACert        string
	DialTimeout   time.Duration
	DialKeepAlive time.Duration
	Username      string
	Password      string
	Ch            chan DicoveryData
	Gid           string
}

var ec client

// NewClient returns Client with a connection to the named machines. It will
// return an error if a connection to the cluster cannot be made.
func NewClient(ctx context.Context, machines []string, options ClientOptions) (Client, error) {
	if options.DialTimeout == 0 {
		options.DialTimeout = 3 * time.Second
	}
	if options.DialKeepAlive == 0 {
		options.DialKeepAlive = 3 * time.Second
	}

	var err error
	var tlscfg *tls.Config

	if options.Cert != "" && options.Key != "" {
		tlsInfo := transport.TLSInfo{
			CertFile:      options.Cert,
			KeyFile:       options.Key,
			TrustedCAFile: options.CACert,
		}
		tlscfg, err = tlsInfo.ClientConfig()
		if err != nil {
			return nil, err
		}
	}

	cli, err := clientv3.New(clientv3.Config{
		Context:           ctx,
		Endpoints:         machines,
		DialTimeout:       options.DialTimeout,
		DialKeepAliveTime: options.DialKeepAlive,
		TLS:               tlscfg,
		Username:          options.Username,
		Password:          options.Password,
	})
	if err != nil {
		return nil, err
	}

	ec.cli = cli
	ec.ctx = ctx
	ec.kv = clientv3.NewKV(cli)
	ec.ch = options.Ch
	ec.gid = options.Gid

	return &ec, nil
}

func (c *client) LeaseID(id int64) int64 { return int64(c.leaseId) }

// GetEntries implements the etcd Client interface.
func (c *client) GetEntries(key string) (*clientv3.GetResponse, error) {
	resp, err := c.kv.Get(c.ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// WatchPrefix implements the etcd Client interface.
func (c *client) WatchPrefix(prefix string, ch chan clientv3.WatchResponse) {
	c.wctx, c.wcf = context.WithCancel(c.ctx)
	c.watcher = clientv3.NewWatcher(c.cli)

	wch := c.watcher.Watch(c.wctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(0))

	for {
		select {
		case resp := <-wch:
			ch <- resp
		}
	}
}

func (c *client) Register(s Service) (int64, error) {
	var err error

	if s.Key == "" {
		return 0, ErrNoKey
	}
	if s.Value == "" {
		return 0, ErrNoValue
	}

	reg := &register{}

	reg.leaser = clientv3.NewLease(c.cli)

	if c.watcher == nil {
		c.watcher = clientv3.NewWatcher(c.cli)
	}

	if c.kv == nil {
		c.kv = clientv3.NewKV(c.cli)
	}

	if s.TTL == nil {
		s.TTL = NewTTLOption(time.Second*3, time.Second*10)
	}

	grantResp, err := reg.leaser.Grant(c.ctx, int64(s.TTL.ttl.Seconds()))
	if err != nil {
		return 0, err
	}

	reg.leaseID = grantResp.ID

	_, err = c.kv.Put(
		c.ctx,
		s.Key,
		s.Value,
		clientv3.WithLease(reg.leaseID),
	)
	if err != nil {
		return 0, err
	}

	// this will keep the key alive 'forever' or until we revoke it or
	// the context is canceled
	reg.hbch, err = reg.leaser.KeepAlive(c.ctx, reg.leaseID)
	if err != nil {
		return 0, err
	}

	// discard the keepalive response, make etcd library not to complain
	// fix bug #799
	go func() {
		for {
			select {
			case r := <-reg.hbch:
				// mylog.Error("register error ", reg.leaseID)
				// avoid dead loop when channel was closed
				if r == nil {
					return
				}
			case <-c.ctx.Done():
				// mylog.Error("ctx error ", reg.leaseID)

				return
			}
		}
	}()

	c.leaseId = reg.leaseID

	return int64(reg.leaseID), nil
}

func (c *client) Deregister(id int64, s Service) error {
	if s.Key == "" {
		return ErrNoKey
	}
	if _, err := c.cli.Delete(c.ctx, s.Key, clientv3.WithIgnoreLease()); err != nil {
		return err
	}

	return nil
}

// close will close any open clients and call
// the watcher cancel func
func (c *client) close(id int64) {
	if c.watcher != nil {
		c.watcher.Close()
	}
	if c.wcf != nil {
		c.wcf()
	}
}
