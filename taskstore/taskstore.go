package taskstore

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"go.etcd.io/bbolt"
	bolt "go.etcd.io/bbolt"
)

var db *bbolt.DB

const bucketName = "tasks"

// RegisterTaskDB creates bucket tasks in provided DB
func RegisterTaskDB(taskDB *bbolt.DB) error {
	db = taskDB
	return db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(bucketName)); err != nil {
			return err
		}
		return nil
	})
}

// OpenTasksDB returns bolt DB referense or nil with error
func OpenTasksDB(path string) (*bbolt.DB, error) {
	taskDB, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return nil, err
	}
	return taskDB, nil
}

// CloseTasksDB calls Close() DB method along with returning error from Close()
func CloseTasksDB() error {
	return db.Close()
}

// AddTask stores string in DB with auto-increment int64 key
func AddTask(task string) error {
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		id, err := b.NextSequence()
		if err != nil {
			return err
		}
		if err := b.Put(itob(int(id)), []byte(task)); err != nil {
			return err
		}
		return nil
	})
}

// ListTask returns all tasks in formatted presentation
func ListTask() string {
	sbuf := bytes.NewBufferString("")
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			fmt.Fprint(sbuf, fmt.Sprintf("%v. %s\n", binary.BigEndian.Uint64(k), v))
		}

		return nil
	})
	return sbuf.String()
}

// RemoveTask removes task with particular key
func RemoveTask(key int) (string, bool) {
	var v []byte
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		v = b.Get(itob(key))
		return nil
	})
	err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if err := b.Delete(itob(key)); err != nil {
			return err
		}
		return nil
	})
	if v != nil && err == nil {
		return string(v), true
	}
	return "", false
}

// itob returns an 8-byte big endian representation of v.
func itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}
