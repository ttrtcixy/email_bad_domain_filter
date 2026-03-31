package emailfilter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type Config struct {
	UpdateTime time.Duration `env:"EMAIL_FILTER_FILE_UPDATE_DURATION,required"`
	Url        string        `env:"EMAIL_FILTER_FILE_URL,required"`
	UpdateTask bool          `env:"EMAIL_FILTER_UPDATE_TASK"                   envDefault:"true"`
}
type Filter struct {
	storage atomic.Pointer[storage]

	log *slog.Logger
	cfg *Config

	client  *http.Client
	request *http.Request

	eTag string

	mu sync.Mutex
}

type storage struct {
	data    []byte
	offsets []uint32
}

var defaultStore = &storage{
	data:    make([]byte, 0),
	offsets: make([]uint32, 0),
}

func New(ctx context.Context, log *slog.Logger, cfg *Config) (es *Filter, err error) {
	const op = "emailfilter.New"

	es = &Filter{
		log: log,
		cfg: cfg,
		mu:  sync.Mutex{},
	}

	es.storage.Store(defaultStore)

	req, err := http.NewRequest(http.MethodGet, cfg.Url, nil)
	if err != nil {
		return nil, fmt.Errorf("%s -> %w", op, err)
	}

	es.request = req

	es.client = &http.Client{Timeout: 15 * time.Second}

	if err = es.refresh(ctx); err != nil {
		return nil, fmt.Errorf("%s -> %w", op, err)
	}

	if es.cfg.UpdateTask {
		go es.updater(ctx)
	}

	return es, nil
}

func (f *Filter) IsDomainValid(domain string) bool {
	st := f.storage.Load()
	if st == nil || len(st.offsets) == 0 {
		return true
	}

	n := len(st.offsets)

	// danger
	targetBytes := unsafe.Slice(unsafe.StringData(domain), len(domain))

	idx := sort.Search(n, func(i int) bool {
		start := st.offsets[i]
		end := uint32(len(st.data))

		if i+1 < n {
			end = st.offsets[i+1]
		}

		return bytes.Compare(st.data[start:end], targetBytes) >= 0
	})

	if idx < n {
		start := st.offsets[idx]
		end := uint32(len(st.data))

		if idx+1 < n {
			end = st.offsets[idx+1]
		}

		if bytes.Equal(st.data[start:end], targetBytes) {
			return false
		}
	}

	return true
}

func (f *Filter) updater(ctx context.Context) {
	const op = "emailfilter.updater"

	ticker := time.NewTicker(f.cfg.UpdateTime)

	for {
		select {
		case <-ticker.C:
			if err := f.refresh(ctx); err != nil {
				f.log.LogAttrs(ctx, slog.LevelError, "Get file disposable email domains error",
					slog.String("op", op),
					slog.String("error", err.Error()),
				)
			}
		case <-ctx.Done():
			return
		}
	}
}

var ErrNothingToUpdate = errors.New("nothing to update")

func (f *Filter) refresh(ctx context.Context) (err error) {
	const op = "emailfilter.refresh"

	f.mu.Lock()
	defer f.mu.Unlock()

	buf, err := f.fetchLatestData(ctx)
	if err != nil {
		if errors.Is(err, ErrNothingToUpdate) {
			return nil
		}

		return fmt.Errorf("%s -> %w", op, err)
	}

	newStorage := f.buildStorage(ctx, buf)

	f.storage.Store(newStorage)

	f.log.LogAttrs(ctx, slog.LevelDebug, "Successful update bad email domain list", slog.Int("byte_count", f.size()))

	return nil
}

func (f *Filter) fetchLatestData(ctx context.Context) ([]byte, error) {
	const op = "emailfilter.fetchLatestData"

	if f.eTag != "" {
		f.request.Header.Set("If-None-Match", f.eTag)
	}

	resp, err := f.client.Do(f.request.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("%s -> %w", op, err)
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode == http.StatusNotModified {
		return nil, ErrNothingToUpdate
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	etag := resp.Header.Get("ETag")
	if etag != "" {
		f.eTag = etag
	}

	if resp.ContentLength == 0 {
		return nil, errors.New("empty response body")
	}

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("%s -> %w", op, err)
	}

	return buf, nil
}

func (f *Filter) buildStorage(_ context.Context, buf []byte) *storage {
	st := storage{
		data: make([]byte, 0, len(buf)),
		// write first offset
		offsets: make([]uint32, 1, len(buf)/10),
	}

	buf = bytes.TrimSpace(buf)

	var offset uint32

	for i := 0; i < len(buf); i++ {
		val := buf[i]
		if val == '\n' {
			// if already write offset
			if st.offsets[len(st.offsets)-1] == offset {
				continue
			}

			st.offsets = append(st.offsets, offset)
			continue
		}

		if val == ' ' || val == '\r' || val == '\t' {
			continue
		}

		// write byte
		st.data = append(st.data, val)
		offset++
	}

	var data = make([]byte, len(st.data))
	copy(data, st.data)
	var dataOffset = make([]uint32, len(st.offsets))
	copy(dataOffset, st.offsets)

	st.data = data
	st.offsets = dataOffset

	return &st
}

// for data = []byte and offsets = []uint32
func (f *Filter) size() int {
	storage := *f.storage.Load()

	var sliceDataSize = int(unsafe.Sizeof(storage.data))

	var sliceOffsetSize = int(unsafe.Sizeof(storage.offsets))

	var dataSize = len(storage.data)

	var offsetSize = len(storage.offsets) * 4

	return sliceDataSize + sliceOffsetSize + dataSize + offsetSize
}
