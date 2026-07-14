package cloud

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/yowza/yowza"
)

// S3Backend implements StorageBackend using AWS S3.
// WALs are appended to an object (requires reading and rewriting, or batching to files).
// For simplicity in S3, WAL appends are batched to discrete objects (e.g. wal_TIMESTAMP.json),
// and compaction merges them into the snap object.
type S3Backend struct {
	client *s3.Client
	bucket string
	prefix string
	ctx    context.Context
}

func NewS3Backend(bucket, prefix string) (*S3Backend, error) {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("unable to load SDK config, %v", err)
	}

	client := s3.NewFromConfig(cfg)

	return &S3Backend{
		client: client,
		bucket: bucket,
		prefix: prefix,
		ctx:    context.Background(),
	}, nil
}

func (b *S3Backend) SaveWAL(entries []yowza.WalEntry) error {
	if len(entries) == 0 {
		return nil
	}

	// Create a batched WAL object
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	for _, entry := range entries {
		if err := encoder.Encode(entry); err != nil {
			return err
		}
	}

	key := fmt.Sprintf("%s/wal/batch_%d.json", b.prefix, time.Now().UnixNano())

	_, err := b.client.PutObject(b.ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf.Bytes()),
	})

	return err
}

func (b *S3Backend) SaveSnapshot(data []byte) error {
	key := fmt.Sprintf("%s/snapshot.bin", b.prefix)

	_, err := b.client.PutObject(b.ctx, &s3.PutObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return err
	}

	// Delete the WAL batches compacted into this snapshot, or Bootstrap
	// would replay every pre-snapshot batch on the next start (the
	// step-3 replay made this a correctness bug, not just wasted space).
	// The prefix is exactly the one SaveWAL writes and LoadWAL filters:
	// "<prefix>/wal/batch_<UnixNano>.json".
	// Error policy: this follows the rest of this file (LoadWAL/SaveWAL
	// return S3 errors, they are not best-effort), but deletion continues
	// past a single failed object and only the first error is returned,
	// so one bad object does not block cleanup of the others. A returned
	// error means the caller must NOT assume the WAL is clean.
	walPrefix := fmt.Sprintf("%s/wal/batch_", b.prefix)
	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(walPrefix),
	})
	var firstErr error
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(b.ctx)
		if err != nil {
			return fmt.Errorf("failed to list S3 WAL batches for cleanup: %w", err)
		}
		for _, obj := range page.Contents {
			_, err := b.client.DeleteObject(b.ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(b.bucket),
				Key:    obj.Key,
			})
			if err != nil && firstErr == nil {
				firstErr = fmt.Errorf("failed to delete S3 WAL batch %s: %w", aws.ToString(obj.Key), err)
			}
		}
	}

	return firstErr
}

// isS3NotFound reports whether an S3 error means "this object does not
// exist", as opposed to a real failure (network error, AccessDenied, ...).
// It checks the smithy error code, not the error string: S3 reports a
// missing key as NoSuchKey (and some S3-compatible stores as NotFound).
func isS3NotFound(err error) bool {
	var noSuchKey *types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		code := apiErr.ErrorCode()
		return code == "NoSuchKey" || code == "NotFound"
	}
	return false
}

func (b *S3Backend) LoadSnapshot() ([]byte, error) {
	key := fmt.Sprintf("%s/snapshot.bin", b.prefix)

	out, err := b.client.GetObject(b.ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		// Only a genuine "no such object" means "no snapshot yet".
		// Any other AWS/network/AccessDenied error is returned as an
		// error: silently treating it as "starting fresh" would discard
		// the existing database state on the next save.
		if isS3NotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to load snapshot from S3: %w", err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// LoadWAL lists the wal/batch_*.json objects in timestamp order (the
// UnixNano suffix SaveWAL embeds in the key) and decodes their NDJSON
// entries in that order, so bootstrap replay applies batches oldest-first.
// A bucket with no WAL batches returns nil, nil.
func (b *S3Backend) LoadWAL() ([]yowza.WalEntry, error) {
	walPrefix := fmt.Sprintf("%s/wal/", b.prefix)

	paginator := s3.NewListObjectsV2Paginator(b.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(walPrefix),
	})

	var keys []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(b.ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to list WAL batches from S3: %w", err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			if strings.HasPrefix(key, walPrefix+"batch_") && strings.HasSuffix(key, ".json") {
				keys = append(keys, key)
			}
		}
	}

	// Sort by the embedded UnixNano timestamp, then by key as a
	// tiebreaker (two batches in the same nanosecond, or a clock jump).
	sort.Slice(keys, func(i, j int) bool {
		ti := walBatchTimestamp(keys[i])
		tj := walBatchTimestamp(keys[j])
		if ti != tj {
			return ti < tj
		}
		return keys[i] < keys[j]
	})

	var entries []yowza.WalEntry
	for _, key := range keys {
		out, err := b.client.GetObject(b.ctx, &s3.GetObjectInput{
			Bucket: aws.String(b.bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to load WAL batch %s from S3: %w", key, err)
		}
		data, err := io.ReadAll(out.Body)
		out.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read WAL batch %s from S3: %w", key, err)
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		for decoder.More() {
			var entry yowza.WalEntry
			if err := decoder.Decode(&entry); err != nil {
				return nil, fmt.Errorf("failed to decode WAL batch %s: %w", key, err)
			}
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

// walBatchTimestamp extracts the UnixNano suffix from a
// "<prefix>/wal/batch_<nanos>.json" key, or 0 if it does not parse.
func walBatchTimestamp(key string) int64 {
	base := key[strings.LastIndex(key, "/")+1:]
	if !strings.HasPrefix(base, "batch_") || !strings.HasSuffix(base, ".json") {
		return 0
	}
	n, err := strconv.ParseInt(base[len("batch_"):len(base)-len(".json")], 10, 64)
	if err != nil {
		return 0
	}
	return n
}
