package rest

import (
	"fmt"
	"quark"
	"quark/httpcmd"
)

type RestService interface {
	SupportedResources() []Resource
	HandleGet(ResourceStore, *quark.Task) *quark.TaskResult
	HandlePost(ResourceStore, *quark.Task) *quark.TaskResult
	HandlePut(ResourceStore, *quark.Task) *quark.TaskResult
	HandleDelete(ResourceStore, *quark.Task) *quark.TaskResult
	HandlePatch(ResourceStore, *quark.Task) *quark.TaskResult
}

type restServiceAdaptor struct {
	RestService
	store ResourceStore
}

func NewRestServiceAdaptor(s RestService, store ResourceStore) quark.Service {
	return &restServiceAdaptor{
		RestService: s,
		store:       store,
	}
}

func (s *restServiceAdaptor) HandleTask(t *quark.Task) *quark.TaskResult {
	store := s.store
	switch t.Cmds[0].(type) {
	case *GetCmd:
		return s.HandleGet(store, t)
	case *PostCmd:
		for _, c_ := range t.Cmds {
			c, _ := c_.(*PostCmd)
			if err := c.NewResource.Validate(); err != nil {
				return t.Failed(err)
			}
		}
		return s.HandlePost(store, t)
	case *DeleteCmd:
		tx, _ := store.Begin()
		defer tx.Commit()
		for _, c_ := range t.Cmds {
			c, _ := c_.(*DeleteCmd)
			count, err := tx.Count(ResourceType(c.ResourceType), map[string]interface{}{"id": c.Id})
			if err != nil {
				return t.Failed(fmt.Errorf("db get delete resource failed:%v", err))
			} else if count == 0 {
				return t.Failed(fmt.Errorf("delete unknown resource: %s", c.ResourceType))
			}
		}
		return s.HandleDelete(store, t)
	case *PutCmd:
		tx, _ := store.Begin()
		defer tx.Commit()
		for _, c_ := range t.Cmds {
			c, _ := c_.(*PutCmd)
			if err := c.NewResource.Validate(); err != nil {
				return t.Failed(err)
			}

			rt := GetResourceType(c.NewResource)
			id := ResourceID(c.NewResource)
			count, err := tx.Count(rt, map[string]interface{}{"id": id})
			if err != nil {
				return t.Failed(fmt.Errorf("db get update resource failed:%v", err))
			} else if count == 0 {
				return t.Failed(fmt.Errorf("update unknown resource: %s", rt))
			}
		}
		return s.HandlePut(store, t)
	case *PatchCmd:
		tx, _ := store.Begin()
		defer tx.Commit()
		for _, c_ := range t.Cmds {
			c, _ := c_.(*PatchCmd)
			count, err := tx.Count(ResourceType(c.ResourceType), map[string]interface{}{"id": c.Id})
			if err != nil {
				return t.Failed(fmt.Errorf("db get update resource failed:%v", err))
			} else if count == 0 {
				return t.Failed(fmt.Errorf("update unknown resource: %s", c.ResourceType))
			}
		}
		return s.HandlePatch(store, t)

	default:
		panic("unknown task")
	}
}

func (s *restServiceAdaptor) SupportedCmds() []quark.Command {
	return []quark.Command{&GetCmd{}, &PostCmd{}, &PutCmd{}, &DeleteCmd{}}
}

func StoreForResources(resources []Resource, registry quark.Registry) (ResourceStore, error) {
	var dbConf quark.DBConf
	if err := quark.GetDBConf(registry, &dbConf); err != nil {
		return nil, err
	}

	if mr, err := NewResourceMeta(resources); err == nil {
		if store, err := NewRStore(Postgresql, map[string]interface{}{
			"host":     dbConf.Host,
			"user":     dbConf.User,
			"password": dbConf.Pwd,
			"dbname":   dbConf.DB,
		}, mr); err == nil {
			return store, nil
		} else {
			return nil, err
		}
	} else {
		return nil, err
	}
}

func Run(s RestService, registry quark.Registry, e *quark.EndPoint) error {
	store, err := StoreForResources(s.SupportedResources(), registry)
	if err != nil {
		return err
	}

	adaptor := NewRestServiceAdaptor(s, store)
	p, err := NewRestProtocol(s.SupportedResources(), e)
	if err != nil {
		return err
	}

	if err := quark.RegisterService(registry, e); err != nil {
		return err
	}

	httpcmd.NewHttpTransport().Run(adaptor, p, e)
	return nil
}
