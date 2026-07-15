package cloud

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/yowza/yowza"
)

type fakeS3Server struct {
	mu          sync.Mutex
	objects     map[string][]byte
	denyGetSnap bool
}

func newFakeS3Server() (*fakeS3Server, *httptest.Server) {
	f := &fakeS3Server{objects: make(map[string][]byte)}
	srv := httptest.NewServer(http.HandlerFunc(f.handle))
	return f, srv
}

func (f *fakeS3Server) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Path-style URL: /{bucket}/{key...}
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)
	bucket := parts[0]
	key := ""
	if len(parts) > 1 {
		key = parts[1]
	}

	// ListObjectsV2: GET /{bucket}?list-type=2&prefix=...
	if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
		prefix := r.URL.Query().Get("prefix")
		var keys []string
		for k := range f.objects {
			if strings.HasPrefix(k, prefix) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		w.Header().Set("Content-Type", "application/xml")
		var sb strings.Builder
		sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?>`)
		sb.WriteString(fmt.Sprintf(`<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>%s</Name><KeyCount>%d</KeyCount><IsTruncated>false</IsTruncated>`, bucket, len(keys)))
		for _, k := range keys {
			sb.WriteString(fmt.Sprintf(`<Contents><Key>%s</Key><Size>%d</Size></Contents>`, k, len(f.objects[k])))
		}
		sb.WriteString(`</ListBucketResult>`)
		_, _ = w.Write([]byte(sb.String()))
		return
	}

	switch r.Method {
	case http.MethodPut:
		body, _ := io.ReadAll(r.Body)
		f.objects[key] = body
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		if f.denyGetSnap && strings.HasSuffix(key, "snapshot.bin") {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`))
			return
		}
		data, ok := f.objects[key]
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	case http.MethodDelete:
		delete(f.objects, key)
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func TestS3BackendEndToEndAndWALCleanup(t *testing.T) {
	fake, srv := newFakeS3Server()
	defer srv.Close()

	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(srv.URL),
		UsePathStyle: true,
		Credentials:  aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider("test", "test", "")),
	})

	backend := &S3Backend{
		client: client,
		bucket: "yowza-test-bucket",
		prefix: "prod",
		ctx:    context.Background(),
	}

	// 1. Missing snapshot returns (nil, nil)
	snap, err := backend.LoadSnapshot()
	if err != nil || snap != nil {
		t.Fatalf("expected (nil, nil) on missing snapshot, got snap=%v err=%v", snap, err)
	}

	// 2. AccessDenied on LoadSnapshot MUST return an error, not (nil, nil)
	fake.denyGetSnap = true
	_, err = backend.LoadSnapshot()
	if err == nil {
		t.Fatalf("expected error on AccessDenied LoadSnapshot, got nil")
	}
	fake.denyGetSnap = false

	// 3. Save two WAL batches and verify LoadWAL returns them in order
	if err := backend.SaveWAL([]yowza.WalEntry{{Opcode: yowza.OpcodePut, Key: []byte("k1"), Value: []byte("v1")}}); err != nil {
		t.Fatalf("SaveWAL 1 failed: %v", err)
	}
	if err := backend.SaveWAL([]yowza.WalEntry{{Opcode: yowza.OpcodePut, Key: []byte("k2"), Value: []byte("v2")}}); err != nil {
		t.Fatalf("SaveWAL 2 failed: %v", err)
	}
	entries, err := backend.LoadWAL()
	if err != nil || len(entries) != 2 {
		t.Fatalf("expected 2 WAL entries, got %d (err=%v)", len(entries), err)
	}
	if string(entries[0].Key) != "k1" || string(entries[1].Key) != "k2" {
		t.Fatalf("unexpected WAL order: %+v", entries)
	}

	// 4. SaveSnapshot must upload snapshot.bin AND delete all wal/batch_*.json objects
	if err := backend.SaveSnapshot([]byte("SNAPDATA")); err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}
	loadedSnap, err := backend.LoadSnapshot()
	if err != nil || string(loadedSnap) != "SNAPDATA" {
		t.Fatalf("LoadSnapshot mismatch: %q err=%v", loadedSnap, err)
	}
	entriesAfterSnap, err := backend.LoadWAL()
	if err != nil || len(entriesAfterSnap) != 0 {
		t.Fatalf("expected 0 WAL entries after SaveSnapshot cleanup, got %d (err=%v)", len(entriesAfterSnap), err)
	}
}
