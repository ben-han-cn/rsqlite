package rest

import (
	"os"
	"testing"
	"time"

	ut "cement/unittest"
)

var sqlite3DBConnInfo = map[string]interface{}{
	"path": "boy_sqlit3.db",
}

type boy struct {
	Id       string
	Name     string `sql:"uk"`
	Age      uint32
	Birthday time.Time
	Talented bool
}

func (b *boy) Validate() error {
	return nil
}

func TestSqlite3CURDRecord(t *testing.T) {
	var store ResourceStore
	var err error

	mr, err := NewResourceMeta([]Resource{&boy{}})
	ut.Assert(t, err == nil, "err should be nil but %v", err)
	store, err = NewRStore(Sqlite3, sqlite3DBConnInfo, mr)
	ut.Equal(t, err, nil)

	tx, _ := store.Begin()
	birthDay := time.Now()
	c := &boy{
		Name:     "ben",
		Age:      20,
		Birthday: birthDay,
		Talented: true,
	}
	r, err := tx.Insert(c)
	newBoy, ok := r.(*boy)
	ut.Equal(t, ok, true)
	ut.NotEqual(t, newBoy.Id, "")

	boys := []boy{}
	tx.Fill(map[string]interface{}{"id": newBoy.Id}, &boys)
	ut.Equal(t, len(boys), 1)
	ut.Equal(t, boys[0].Birthday.Unix(), birthDay.Unix())
	tx.Commit()

	tx, _ = store.Begin()
	c = &boy{
		Id:       "xxxxxxx",
		Name:     "benxxxx",
		Age:      20,
		Birthday: time.Now(),
		Talented: true,
	}

	_, err = tx.Insert(c)
	ut.Equal(t, err, nil)

	boys = []boy{}
	tx.Fill(map[string]interface{}{"id": "xxxxxxx"}, &boys)
	ut.Equal(t, len(boys), 1)
	tx.Delete("boy", map[string]interface{}{"id": "xxxxxxx"})
	tx.Commit()

	//tx automatic rollback
	tx, _ = store.Begin()
	c = &boy{
		Name:     "nana",
		Age:      20,
		Birthday: time.Now(),
		Talented: false,
	}
	tx.Insert(c)
	c = &boy{
		Name:     "ben",
		Age:      20,
		Birthday: time.Now(),
		Talented: false,
	}
	_, err = tx.Insert(c)
	ut.NotEqual(t, err, nil)
	err = tx.RollBack()
	ut.Equal(t, err, nil)

	tx, _ = store.Begin()
	boys = []boy{}
	tx.Fill(map[string]interface{}{"Age": 20}, &boys)
	ut.Equal(t, len(boys), 1)

	c = &boy{
		Name:     "nana",
		Age:      20,
		Birthday: time.Now(),
		Talented: true,
	}
	tx.Insert(c)
	studentsInterface, err := tx.Get("boy", map[string]interface{}{"Age": 20})
	ut.Equal(t, err, nil)

	boys, ok = studentsInterface.([]boy)
	ut.Equal(t, ok, true)

	ut.Equal(t, len(boys), 2)
	for _, s := range boys {
		ut.Assert(t, s.Talented, "boy talented isn't stored correctly")
	}

	existsBoy, _ := tx.Exists("boy", map[string]interface{}{"Talented": false})
	ut.Equal(t, existsBoy, false)
	existsBoy, _ = tx.Exists("boy", map[string]interface{}{"Talented": true})
	ut.Equal(t, existsBoy, true)
	existsBoy, _ = tx.Exists("boy", map[string]interface{}{"Age": 20})
	ut.Equal(t, existsBoy, true)
	existsBoy, _ = tx.Exists("boy", map[string]interface{}{"Age": 2000})
	ut.Equal(t, existsBoy, false)

	rows, err := tx.Update("boy", map[string]interface{}{"Age": uint32(0xffff)}, map[string]interface{}{"Name": "ben"})
	ut.Equal(t, err, nil)
	ut.Equal(t, rows, int64(1))

	boys = []boy{}
	err = tx.Fill(map[string]interface{}{"Age": uint32(0xffff)}, &boys)
	ut.Equal(t, err, nil)
	ut.Equal(t, len(boys), 1)
	ut.NotEqual(t, boys[0].Id, "")

	rows, err = tx.Delete("boy", map[string]interface{}{"Age": 20})
	ut.Equal(t, err, nil)
	ut.Equal(t, rows, int64(1))

	rows, err = tx.Delete("boy", map[string]interface{}{"Age": 20})
	ut.Equal(t, rows, int64(0))

	boys = []boy{}
	err = tx.Fill(map[string]interface{}{"Age": 20}, &boys)
	ut.Equal(t, err, nil)
	ut.Equal(t, len(boys), 0)

	rows, err = tx.Delete("boy", map[string]interface{}{})
	ut.Equal(t, err, nil)
	ut.Equal(t, rows, int64(1))

	boys = []boy{}
	err = tx.Fill(map[string]interface{}{}, &boys)
	ut.Equal(t, err, nil)
	ut.Equal(t, len(boys), 0)
	tx.RollBack()

	store.Clean()
	tx, _ = store.Begin()
	boys = []boy{}
	err = tx.Fill(map[string]interface{}{}, &boys)
	//ut.Equal(t, err, nil)
	ut.Equal(t, len(boys), 0)
	tx.RollBack()

	store.Destroy()
	os.Remove(sqlite3DBConnInfo["path"].(string))
}

func TestSqlite3ForeignerKey(t *testing.T) {
	var store ResourceStore
	var err error

	mr, err := NewResourceMeta([]Resource{&tuser{}, &tview{}, &tuserTview{}})
	ut.Assert(t, err == nil, "err should be nil but %v", err)
	store, err = NewRStore(Sqlite3, sqlite3DBConnInfo, mr)
	ut.Equal(t, err, nil)
	defer os.Remove(sqlite3DBConnInfo["path"].(string))

	tx, _ := store.Begin()
	user, _ := tx.Insert(&tuser{
		Name: "ben",
	})

	view, _ := tx.Insert(&tview{
		Name: "v1",
	})

	tx.Insert(&tuserTview{
		Tuser: user.(*tuser).Id,
		Tview: view.(*tview).Id,
	})
	tx.Commit()

	tx, _ = store.Begin()
	userViews := []tuserTview{}
	tx.Fill(map[string]interface{}{}, &userViews)
	ut.Equal(t, len(userViews), 1)

	rows, _ := tx.Delete("tuser", map[string]interface{}{"name": "ben"})
	ut.Equal(t, rows, int64(1))
	tx.Commit()

	tx, _ = store.Begin()
	userViews = []tuserTview{}
	tx.Fill(map[string]interface{}{}, &userViews)
	ut.Equal(t, len(userViews), 0)
	tx.RollBack()

	store.Destroy()
}
