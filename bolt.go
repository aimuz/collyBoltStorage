package colly_bolt_storage

import (
	"fmt"
	"net/url"
	"sync"
	"log"
	"github.com/coreos/bbolt"
	"os"
	"errors"
	"encoding/binary"
)

var (
	CookiesErrNil = errors.New("Cookies() .Get error is null ")
)

// Storage implements the redis storage backend for Colly
type Storage struct {
	Path       string
	Mode       os.FileMode
	Options    *bolt.Options
	Password   string
	Prefix     string
	BucketName []byte
	DB         *bolt.DB
	mu         sync.RWMutex // Only used for cookie methods.
}

// Init initializes the bolt storage
func (s *Storage) Init() (err error) {
	if s.DB == nil {
		s.DB, err = bolt.Open(s.Path, s.Mode, s.Options)
	}
	if err != nil {
		return err
	}
	return s.DB.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(s.BucketName)
		if err != nil {
			return err
		}
		_, err = tx.Bucket(s.BucketName).CreateBucketIfNotExists(s.getQueueID())
		return err
	})

}

// Clear removes all entries from the storage
func (s *Storage) Clear() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.DB.Update(func(tx *bolt.Tx) error {
		err := tx.DeleteBucket(s.BucketName)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(s.BucketName)
		if err != nil {
			return err
		}
		_, err = tx.Bucket(s.BucketName).CreateBucketIfNotExists(s.getQueueID())
		return err
	})
}

// Visited implements colly/storage.Visited()
func (s *Storage) Visited(requestID uint64) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.BucketName)
		return b.Put(s.getIDStr(requestID), s.itob(1))
	})
}

// IsVisited implements colly/storage.IsVisited()
func (s *Storage) IsVisited(requestID uint64) (bool, error) {
	bl := false
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.BucketName)
		v := b.Get(s.getIDStr(requestID))
		if v == nil {
			bl = true
		}
		return nil
	})
	return bl, err
}

// SetCookies implements colly/storage..SetCookies()
func (s *Storage) SetCookies(u *url.URL, cookies string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.BucketName)
		return b.Put(s.getCookieID(u.Host), []byte(cookies))
	})
	if err != nil {
		log.Printf("SetCookies() .Set error %s", err)
		return
	}
}

// Cookies implements colly/storage.Cookies()
func (s *Storage) Cookies(u *url.URL) string {
	s.mu.RLock()
	cookies := ""
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.BucketName)
		v := b.Get(s.getCookieID(u.Host))
		if v == nil {
			return CookiesErrNil
		}
		cookies = string(b.Get(s.getCookieID(u.Host)))
		return nil
	})
	s.mu.RUnlock()
	if err == CookiesErrNil {
		log.Println(err)
		return ""
	}
	return cookies
}

// AddRequest implements queue.Storage.AddRequest() function
func (s *Storage) AddRequest(r []byte) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.BucketName).Bucket(s.getQueueID())
		id, err := b.NextSequence()
		if err != nil {
			return err
		}
		return b.Put(s.itob(int(id)), r)
	})
}

// GetRequest implements queue.Storage.GetRequest() function
func (s *Storage) GetRequest() ([]byte, error) {
	var value []byte
	err := s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.BucketName).Bucket(s.getQueueID())
		c := b.Cursor()
		k, v := c.First()
		if len(k) == 0 && len(v) == 0 {
			return fmt.Errorf("Queue is empty ")
		}

		err := b.Delete(k)
		if err != nil {
			return err
		}
		value = v
		return nil
	})
	return value, err
}

// QueueSize implements queue.Storage.QueueSize() function
func (s *Storage) QueueSize() (int, error) {
	var n int
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(s.BucketName).Bucket(s.getQueueID())
		n = b.Stats().KeyN
		return nil
	})
	return n, err
}

func (s *Storage) getIDStr(ID uint64) []byte {
	return []byte(fmt.Sprintf("%s:request:%d", s.Prefix, ID))
}

func (s *Storage) getCookieID(c string) []byte {
	return []byte(fmt.Sprintf("%s:cookie:%s", s.Prefix, c))
}

func (s *Storage) getQueueID() []byte {
	return []byte(fmt.Sprintf("%s:queue", s.Prefix))
}

func (s *Storage) itob(v int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}
