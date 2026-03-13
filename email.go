package emailvalidator

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
	UpdateTime time.Duration `env:"EMAIL_SERVICE_FILE_UPDATE_DURATION,required"`
	Url        string        `env:"EMAIL_SERVICE_FILE_URL,required"`
	UpdateTask bool          `env:"EMAIL_SERVICE_UPDATE_TASK"                   envDefault:"true"`
}
type EmailService struct {
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

func New(ctx context.Context, log *slog.Logger, cfg *Config) (es *EmailService, err error) {
	const op = "emailservice.New"

	es = &EmailService{
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

func (s *EmailService) IsDomainValid(domain string) bool {
	st := s.storage.Load()
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

func (s *EmailService) updater(ctx context.Context) {
	const op = "emailservice.updater"

	ticker := time.NewTicker(s.cfg.UpdateTime)

	for {
		select {
		case <-ticker.C:
			if err := s.refresh(ctx); err != nil {
				s.log.LogAttrs(ctx, slog.LevelError, "Get file disposable email domains error",
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

func (s *EmailService) refresh(ctx context.Context) (err error) {
	const op = "emailservice.refresh"

	s.mu.Lock()
	defer s.mu.Unlock()

	buf, err := s.fetchLatestData(ctx)
	if err != nil {
		if errors.Is(err, ErrNothingToUpdate) {
			return nil
		}

		return fmt.Errorf("%s -> %w", op, err)
	}

	newStorage := s.buildStorage(ctx, buf)

	s.storage.Store(newStorage)

	s.log.LogAttrs(ctx, slog.LevelInfo, "Successful update bad email domain list", slog.Int("byte_count", s.size()))

	return nil
}

func (s *EmailService) fetchLatestData(ctx context.Context) ([]byte, error) {
	const op = "emailservice.fetchLatestData"

	if s.eTag != "" {
		s.request.Header.Set("If-None-Match", s.eTag)
	}

	resp, err := s.client.Do(s.request.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("%s -> %w", op, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		return nil, ErrNothingToUpdate
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	etag := resp.Header.Get("ETag")
	if etag != "" {
		s.eTag = etag
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

func (s *EmailService) buildStorage(_ context.Context, buf []byte) *storage {
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
func (s *EmailService) size() int {
	storage := *s.storage.Load()

	var sliceDataSize = int(unsafe.Sizeof(storage.data))

	var sliceOffsetSize = int(unsafe.Sizeof(storage.offsets))

	var dataSize = len(storage.data)

	var offsetSize = len(storage.offsets) * 4

	return sliceDataSize + sliceOffsetSize + dataSize + offsetSize
}
