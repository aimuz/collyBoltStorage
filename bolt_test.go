package collyBoltStorage

import (
	"net/url"
	"strings"
	"testing"
)

func TestQueue(t *testing.T) {
	s := &Storage{
		Path:       "test.db",
		Mode:       0700,
		BucketName: []byte("test"),
		Prefix:     "test",
	}
	if err := s.Init(); err != nil {
		t.Error("failed to initialize client: " + err.Error())
		return
	}
	defer s.Clear()
	urls := []string{"http://example.com/", "http://go-colly.org/", "https://xx.yy/zz"}
	for _, u := range urls {
		if err := s.AddRequest([]byte(u)); err != nil {
			t.Error("failed to add request: " + err.Error())
			return
		}
	}
	if size, err := s.QueueSize(); size != 3 || err != nil {
		t.Error("invalid queue size")
		return
	}
	for _, u := range urls {
		if r, err := s.GetRequest(); err != nil || string(r) != u {
			t.Error("failed to get request: " + err.Error())
			return
		}
	}
}

func TestStorage_Visited(t *testing.T) {
	s := &Storage{
		Path:       "test.db",
		Mode:       0700,
		BucketName: []byte("test"),
		Prefix:     "test",
	}
	if err := s.Init(); err != nil {
		t.Error("failed to initialize storage backend: " + err.Error())
		return
	}

	ids := []uint64{1, 2, 3, 4, 5}

	for _, u := range ids {
		if err := s.Visited(u); err != nil {
			t.Error("failed to add visit: " + err.Error())
		}
	}

	for _, u := range ids {
		if _, err := s.IsVisited(u); err != nil {
			t.Error("failed to check visit: " + err.Error())
		}
	}
}

func TestStorage_AddRequest(t *testing.T) {
	s := &Storage{
		Path:       "test.db",
		Mode:       0700,
		BucketName: []byte("test"),
		Prefix:     "test",
	}

	if err := s.Init(); err != nil {
		t.Error("failed to initialize storage backend: " + err.Error())
		return
	}

	request1 := []byte{1, 2, 3, 4, 5, 6}
	request2 := []byte{7, 8, 9, 10, 11, 12}

	if err := s.AddRequest(request1); err != nil {
		t.Error("failed to AddRequest" + err.Error())
	}

	if err := s.AddRequest(request2); err != nil {
		t.Error("failed to AddRequest" + err.Error())
	}

}

func TestStorage_QueueSize(t *testing.T) {
	s := &Storage{
		Path:       "test.db",
		Mode:       0700,
		BucketName: []byte("test"),
		Prefix:     "test",
	}

	if err := s.Init(); err != nil {
		t.Error("failed to initialize storage backend: " + err.Error())
		return
	}

	request1 := []byte{1, 2, 3, 4, 5, 6}
	request2 := []byte{7, 8, 9, 10, 11, 12}

	if err := s.AddRequest(request1); err != nil {
		t.Error("failed to AddRequest" + err.Error())
	}

	if err := s.AddRequest(request2); err != nil {
		t.Error("failed to AddRequest" + err.Error())
	}

	if size, err := s.QueueSize(); err != nil {
		t.Error("failed to get queue size: " + err.Error())
		return
	} else if size != 2 {
		t.Errorf("queue size is not correct should be 2 but is: %v", size)
	} else {
		t.Log("queue size is correct")
	}
}

func TestStorage_GetRequest(t *testing.T) {
	count := 0
	s := &Storage{
		Path:       "test.db",
		Mode:       0700,
		BucketName: []byte("test"),
		Prefix:     "test",
	}
	if err := s.Init(); err != nil {
		t.Error("failed to initialize storage backend: " + err.Error())
		return
	}

	request1 := []byte{1, 2, 3, 4, 5, 6}
	request2 := []byte{7, 8, 9, 10, 11, 12}

	if err := s.AddRequest(request1); err != nil {
		t.Error("failed to AddRequest" + err.Error())
	}

	if err := s.AddRequest(request2); err != nil {
		t.Error("failed to AddRequest" + err.Error())
	}

	for {
		b, err := s.GetRequest()
		if b == nil {
			break
		}
		t.Logf("b: %v", b)
		if err != nil {
			t.Error("failed to GetRequest: " + err.Error())
			break
		}

		count += 1
		if count > 2 {
			t.Error("failed to get 2 requests; got more")
			break
		}
	}
}

func TestStorage_Cookies(t *testing.T) {
	s := &Storage{
		Path:       "test.db",
		Mode:       0700,
		BucketName: []byte("test"),
		Prefix:     "test",
	}
	if err := s.Init(); err != nil {
		t.Error("failed to initialize storage backend: " + err.Error())
		return
	}

	cookies := map[string]string{
		"https://example.com/": "cookie:http://example.com/",
		"http://go-colly.org/": "cookie:http://go-colly.org/",
		"https://xx.yy/zz":     "cookie:https://xx.yy/zz",
	}

	for k, v := range cookies {
		URL, err := url.Parse(k)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("key:", k)
		t.Log("value:", v)
		s.SetCookies(URL, cookies[k])
	}

	for k, v := range cookies {
		URL, err := url.Parse(k)
		if err != nil {
			t.Fatal(err)
		}
		cookie := s.Cookies(URL)
		t.Log("key:", k, "value:", v, "cookie:", cookie)
		if !strings.EqualFold(v, cookie) {
			//t.Fatal(v)
			t.Fatal("Cookies() .Get error is null ")
		}
	}

}
