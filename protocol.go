package rest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"cement/serializer"
	"quark"
	"quark/httpcmd"
)

type RestProtocol struct {
	serializer *serializer.Serializer
	endPoint   quark.EndPoint
}

type taskInJson struct {
	ResourceType string            `json:"resource_type"`
	User         string            `json:"zdnsuser"`
	Attrs        []json.RawMessage `json:"attrs"`
}

type GetCmd struct {
	ResourceType string
	Conds        map[string]interface{}
}

type PostCmd struct {
	NewResource Resource
}

type DeleteCmd struct {
	ResourceType string
	Id           string
}

type PutCmd struct {
	NewResource Resource
}

type PatchCmd struct {
	ResourceType string
	Id           string
	NewAttrs     map[string]interface{}
}

func (c *GetCmd) String() string {
	return fmt.Sprintf("getcmd to get %s wich conds %v", c.ResourceType, c.Conds)
}

func (c *PostCmd) String() string {
	return fmt.Sprintf("postcmd to create %v", c.NewResource)
}

func (c *PutCmd) String() string {
	return fmt.Sprintf("putcmd to replace %v", c.NewResource)
}

func (c *DeleteCmd) String() string {
	return fmt.Sprintf("deletecmd to delete %s with id %s", c.ResourceType, c.Id)
}

func (c *PatchCmd) String() string {
	return fmt.Sprintf("patchcmd to update %s with id %s to new val %v", c.ResourceType, c.Id, c.NewAttrs)
}

func NewRestProtocol(resources []Resource, e *quark.EndPoint) (quark.Protocol, error) {
	s := serializer.NewSerializer()
	for _, r := range resources {
		if err := s.Register(r); err != nil {
			return nil, err
		}
	}
	return &RestProtocol{
		serializer: s,
		endPoint:   *e,
	}, nil
}

func (p *RestProtocol) DecodeTask(req interface{}) (*quark.Task, error) {
	r, _ := req.(*http.Request)
	switch r.Method {
	case "GET":
		return p.decodeGetTask(r)
	case "POST":
		return p.decodePostTask(r)
	case "PUT":
		return p.decodePutTask(r)
	case "DELETE":
		return p.decodeDeleteTask(r)
	case "PATCH":
		return p.decodePatchTask(r)
	}
	return nil, fmt.Errorf("unknown http method %v", r.Method)
}

func (p *RestProtocol) decodeGetTask(r *http.Request) (*quark.Task, error) {
	task := quark.NewTask()
	queries := r.URL.Query()
	cmd := &GetCmd{Conds: make(map[string]interface{})}
	var offset, limit int
	var err error
	for k := range queries {
		if k == "resource_type" {
			cmd.ResourceType = queries.Get(k)
			if cmd.ResourceType == "" {
				return nil, fmt.Errorf("empty resource type")
			}
		} else if k == "zdnsuser" {
			task.User = queries.Get(k)
			/* user empty should be avoid, comment here left later to refactor code
			if task.User == "" {
				return nil, fmt.Errorf("empty user")
			}
			*/
		} else if k == "offset" {
			offset, err = strconv.Atoi(queries.Get(k))
			if err != nil {
				return nil, fmt.Errorf("offset isn't a valid integer")
			}
		} else if k == "limit" {
			limit, err = strconv.Atoi(queries.Get(k))
			if err != nil {
				return nil, fmt.Errorf("limit isn't a valid integer")
			}
		} else if k != "_" {
			cmd.Conds[k] = queries.Get(k)
		}
	}

	if limit > 0 && offset >= 0 {
		cmd.Conds["offset"] = offset
		cmd.Conds["limit"] = limit
	}
	task.AddCmd(cmd)
	return task, nil
}

func (p *RestProtocol) decodePostTask(r *http.Request) (*quark.Task, error) {
	body, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("http body reader error %v", err.Error())
	}

	var tt taskInJson
	err = json.Unmarshal(body, &tt)
	if err != nil {
		return nil, err
	}

	t := quark.NewTask()
	t.User = tt.User
	for _, raw := range tt.Attrs {
		resource_, err := p.serializer.DecodeType(tt.ResourceType, raw)
		if err != nil {
			return nil, err
		}
		resource, _ := resource_.(Resource)
		t.AddCmd(&PostCmd{NewResource: resource})
	}
	return t, nil
}

func (p *RestProtocol) decodePutTask(r *http.Request) (*quark.Task, error) {
	body, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("http body reader error %v", err.Error())
	}

	var tt taskInJson
	err = json.Unmarshal(body, &tt)
	if err != nil {
		return nil, err
	}

	t := quark.NewTask()
	t.User = tt.User

	for _, raw := range tt.Attrs {
		resource_, err := p.serializer.DecodeType(tt.ResourceType, raw)
		if err != nil {
			return nil, err
		}
		resource, _ := resource_.(Resource)
		t.AddCmd(&PutCmd{NewResource: resource})
	}
	return t, nil
}

func (p *RestProtocol) decodePatchTask(r *http.Request) (*quark.Task, error) {
	body, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("http body reader error %v", err.Error())
	}

	var tt taskInJson
	err = json.Unmarshal(body, &tt)
	if err != nil {
		return nil, err
	}

	t := quark.NewTask()
	t.User = tt.User
	var patchCmd struct {
		Id       string                 `json:id`
		NewAttrs map[string]interface{} `json:new_attrs`
	}

	for _, raw := range tt.Attrs {
		err := json.Unmarshal(raw, &patchCmd)
		if err != nil {
			return nil, err
		}
		t.AddCmd(&PatchCmd{ResourceType: tt.ResourceType, Id: patchCmd.Id, NewAttrs: patchCmd.NewAttrs})
	}
	return t, nil
}

func (p *RestProtocol) decodeDeleteTask(r *http.Request) (*quark.Task, error) {
	body, err := ioutil.ReadAll(r.Body)
	r.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("http body reader error %v", err.Error())
	}

	var tt taskInJson
	err = json.Unmarshal(body, &tt)
	if err != nil {
		return nil, err
	}

	t := quark.NewTask()
	t.User = tt.User
	for _, raw := range tt.Attrs {
		var id struct {
			Id string `json:"id"`
		}
		json.Unmarshal(raw, &id)
		t.AddCmd(&DeleteCmd{
			ResourceType: tt.ResourceType,
			Id:           id.Id,
		})
	}
	return t, nil
}

type nopCloser struct {
	io.Reader
}

func (nopCloser) Close() error {
	return nil
}

func (p *RestProtocol) EncodeTask(t *quark.Task) (interface{}, error) {
	r := new(http.Request)
	r.ProtoMajor = 1
	r.ProtoMinor = 1
	r.Header = map[string][]string{
		"Content-Type": {"application/json;charset=utf-8"},
		"Accept":       {"*/*"},
	}

	if len(t.Cmds) == 0 {
		return nil, fmt.Errorf("encode empty task")
	}

	firstCmd := t.Cmds[0]
	switch firstCmd.(type) {
	case *GetCmd:
		return p.encodeGetTask(t, r)
	case *PostCmd:
		return p.encodePostTask(t, r)
	case *PutCmd:
		return p.encodePutTask(t, r)
	case *DeleteCmd:
		return p.encodeDeleteTask(t, r)
	case *PatchCmd:
		return p.encodePatchTask(t, r)
	default:
		panic("shouldn't be here")
	}

	return nil, fmt.Errorf("unknown command %v\n", firstCmd)
}

func (p *RestProtocol) encodeGetTask(t *quark.Task, r *http.Request) (interface{}, error) {
	if len(t.Cmds) != 1 {
		return nil, fmt.Errorf("get task doesn't support batch")
	}

	cmd, _ := t.Cmds[0].(*GetCmd)
	uv := url.Values{}
	uv.Set("resource_type", cmd.ResourceType)
	uv.Set("zdnsuser", t.User)
	for k, v := range cmd.Conds {
		uv.Set(k, fmt.Sprintf("%v", v))
	}

	e := &p.endPoint
	uri := e.GenerateServiceUrl() + "?" + uv.Encode()
	r.Method = "GET"
	var err error
	r.URL, err = url.Parse(uri)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (p *RestProtocol) encodePostTask(t *quark.Task, r *http.Request) (interface{}, error) {
	var resourceType string
	var err error
	attrs := make([]json.RawMessage, len(t.Cmds), len(t.Cmds))
	for i, c_ := range t.Cmds {
		c, _ := c_.(*PostCmd)
		if resourceType == "" {
			resourceType = string(GetResourceType(c.NewResource))
		}
		attrs[i], _ = json.Marshal(c.NewResource)
	}
	var tt taskInJson
	tt.User = t.User
	tt.ResourceType = resourceType
	tt.Attrs = attrs
	body, err := json.Marshal(tt)
	if err != nil {
		return nil, err
	}

	r.Method = "POST"
	r.Body = nopCloser{bytes.NewBufferString(string(body))}
	e := &p.endPoint
	r.URL, err = url.Parse(e.GenerateServiceUrl())
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (p *RestProtocol) encodePutTask(t *quark.Task, r *http.Request) (interface{}, error) {
	var resourceType string
	var err error
	attrs := make([]json.RawMessage, len(t.Cmds), len(t.Cmds))
	for i, c_ := range t.Cmds {
		c, _ := c_.(*PutCmd)
		if resourceType == "" {
			resourceType = string(GetResourceType(c.NewResource))
		}
		attrs[i], _ = json.Marshal(c.NewResource)
	}
	var tt taskInJson
	tt.User = t.User
	tt.ResourceType = resourceType
	tt.Attrs = attrs
	body, err := json.Marshal(tt)
	if err != nil {
		return nil, err
	}

	r.Method = "PUT"
	r.Body = nopCloser{bytes.NewBufferString(string(body))}
	e := &p.endPoint
	r.URL, err = url.Parse(e.GenerateServiceUrl())
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (p *RestProtocol) encodeDeleteTask(t *quark.Task, r *http.Request) (interface{}, error) {
	var resourceType string
	var err error
	attrs := make([]json.RawMessage, len(t.Cmds), len(t.Cmds))
	for i, c_ := range t.Cmds {
		c, _ := c_.(*DeleteCmd)
		if resourceType == "" {
			resourceType = c.ResourceType
		}
		var id struct {
			Id string `json:"id"`
		}
		id.Id = c.Id
		attrs[i], _ = json.Marshal(id)
	}
	var tt taskInJson
	tt.User = t.User
	tt.ResourceType = resourceType
	tt.Attrs = attrs
	body, err := json.Marshal(tt)
	if err != nil {
		return nil, err
	}

	r.Method = "DELETE"
	r.Body = nopCloser{bytes.NewBufferString(string(body))}
	e := &p.endPoint
	r.URL, err = url.Parse(e.GenerateServiceUrl())
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (p *RestProtocol) encodePatchTask(t *quark.Task, r *http.Request) (interface{}, error) {
	var resourceType string
	var err error
	attrs := make([]json.RawMessage, len(t.Cmds), len(t.Cmds))
	for i, c_ := range t.Cmds {
		c, _ := c_.(*PatchCmd)
		if resourceType == "" {
			resourceType = c.ResourceType
		}
		var patchcmd struct {
			Id       string                 `json:"id"`
			NewAttrs map[string]interface{} `json:new_attrs`
		}
		patchcmd.Id = c.Id
		patchcmd.NewAttrs = c.NewAttrs
		attrs[i], _ = json.Marshal(patchcmd)
	}

	var tt taskInJson
	tt.User = t.User
	tt.ResourceType = resourceType
	tt.Attrs = attrs
	body, err := json.Marshal(tt)
	if err != nil {
		return nil, err
	}

	r.Method = "PATCH"
	r.Body = nopCloser{bytes.NewBufferString(string(body))}
	e := &p.endPoint
	r.URL, err = url.Parse(e.GenerateServiceUrl())
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (p *RestProtocol) EncodeTaskResult(r *quark.TaskResult) (interface{}, error) {
	return httpcmd.EncodeTaskResult(r)
}

func (p *RestProtocol) DecodeTaskResult(res interface{}, success interface{}, failure interface{}) (*quark.TaskResult, error) {
	response, _ := res.(*http.Response)
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	var result interface{}
	if response.StatusCode == int(quark.Succeed) {
		if success != nil {
			err = json.Unmarshal([]byte(body), success)
			if err != nil {
				return nil, err
			}
			result = success
		}
	} else {
		if failure != nil {
			err = json.Unmarshal([]byte(body), failure)
			if err != nil {
				return nil, err
			}
			result = failure
		}
	}

	return &quark.TaskResult{
		Code:   quark.StatusCode(response.StatusCode),
		Result: result,
	}, nil
}
