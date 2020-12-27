package rest

import (
	"fmt"
	"net/http"
	"time"

	"cement/rest"
	"quark"
)

type RestProxy struct {
	protocol quark.Protocol
	client   *rest.RestClient
}

func NewRestProxy(p quark.Protocol, e *quark.EndPoint) (quark.ServiceProxy, error) {
	client, err := rest.NewRestClient(e.GenerateServiceUrl(), time.Minute)
	if err != nil {
		return nil, err
	}

	return &RestProxy{
		protocol: p,
		client:   client,
	}, nil
}

func (p *RestProxy) HandleTask(t *quark.Task, succeed interface{}, failure interface{}) error {
	req_, err := p.protocol.EncodeTask(t)
	if err != nil {
		return fmt.Errorf("encode task failed:%s", err.Error())
	}

	req, _ := req_.(*http.Request)
	err = p.client.Connect()
	if err != nil {
		return err
	}

	response, _ := p.client.Send(req)
	if response == nil || response.Body == nil {
		err = p.client.ReConnect()
		if err != nil {
			return err
		}

		req_, err := p.protocol.EncodeTask(t)
		if err != nil {
			return err
		}

		response, err = p.client.Send(req_.(*http.Request))
		if response == nil || response.Body == nil {
			return err
		}
	}

	_, err = p.protocol.DecodeTaskResult(response, succeed, failure)
	if err != nil {
		return fmt.Errorf("decode task result failed:%s", err.Error())
	}

	return nil
}

func (p *RestProxy) Close() {
	p.client.Close()
}

func GetProxy(registry quark.Registry, name string, resources []Resource) (quark.ServiceProxy, error) {
	e, err := quark.GetService(registry, name)
	if err != nil {
		return nil, err
	}
	return GetProxyOfService(e, resources)
}

func GetProxyOfService(e *quark.EndPoint, resources []Resource) (quark.ServiceProxy, error) {
	if p, err := NewRestProtocol(resources, e); err != nil {
		return nil, err
	} else {
		return NewRestProxy(p, e)
	}
}
