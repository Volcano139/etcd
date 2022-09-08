package etcd

import (
	"context"
	"time"

	"github.com/coreos/etcd/mvcc/mvccpb"
)

type EtcdHandler interface {
	NewRegistrar(service Service) *Registrar

	Start()
}

type etcd struct {
	cli Client
}

type DicoveryData struct {
	Event mvccpb.Event_EventType //事件类型 put delete
	Info  DiscoveryEtcdInfo
}

type DiscoveryEtcdInfo struct {
	Key   string
	Value string
}

type EtcdInitOptions struct {
	Address []string
	Id      string
	Ch      chan DicoveryData
}

func EtcdInit(options EtcdInitOptions) (EtcdHandler, error) {
	client, err := NewClient(context.TODO(), options.Address, ClientOptions{DialTimeout: time.Second * 3 /*连接最大尝试时间*/, DialKeepAlive: time.Second * 3 /*连接保活时间*/, Gid: options.Id, Ch: options.Ch})
	if err != nil {
		return nil, err
	}

	return etcd{client}, nil
}

func (e etcd) Start() {
	_, err := e.cli.GetEntries("/") //获取etcd数据
	if err != nil {
		return
	}

	go e.cli.WatchPrefix("/", nil)
}

func (e etcd) NewRegistrar(service Service) *Registrar {
	return NewRegistrar(e.cli, service)
}
