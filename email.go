package email_validator

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type Config struct {
	UpdateTime time.Duration `env:"EMAIL_SERVICE_FILE_UPDATE_DURATION,required"`
	Url        string        `env:"EMAIL_SERVICE_FILE_URL,required"`
	FileName   string        `env:"EMAIL_SERVICE_FILE_NAME"                     envDefault:"email_bad_domain_list.txt"`
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

	if err = es.updateFile(ctx); err != nil {
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
	targetBytes := []byte(domain)

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
			if err := s.updateFile(ctx); err != nil {
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

func (s *EmailService) updateFile(ctx context.Context) (err error) {
	const op = "emailservice.updateFile"

	s.mu.Lock()
	defer s.mu.Unlock()

	file, err := os.Create(s.cfg.FileName)
	if err != nil {
		return fmt.Errorf("%s -> %w", op, err)
	}
	defer file.Close()

	if err = s.parseRequest(ctx, file); err != nil {
		if errors.Is(err, ErrNothingToUpdate) {
			return nil
		}

		return fmt.Errorf("%s -> %w", op, err)
	}

	if err = s.parseFile(ctx, file); err != nil {
		return fmt.Errorf("%s -> %w", op, err)
	}

	s.log.LogAttrs(ctx, slog.LevelInfo, "Successful update bad email domain list")

	return nil
}

func (s *EmailService) parseRequest(_ context.Context, file *os.File) error {
	const op = "emailservice.parseRequest"

	if s.eTag != "" {
		s.request.Header.Set("If-None-Match", s.eTag)
	}

	resp, err := s.client.Do(s.request)
	if err != nil {
		return fmt.Errorf("%s -> %w", op, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		return ErrNothingToUpdate
	}

	etag := resp.Header.Get("ETag")
	if etag == "" {
		return errors.New("invalid ETag")
	}

	s.eTag = etag

	if resp.ContentLength == 0 {
		return errors.New("empty response body")
	}

	if _, err := bufio.NewReader(resp.Body).WriteTo(file); err != nil {
		return fmt.Errorf("%s -> %w", op, err)
	}

	if err = s.resetCursor(file); err != nil {
		return err
	}

	return nil
}

func (s *EmailService) parseFile(ctx context.Context, file *os.File) error {
	const op = "emailservice.parseFile"

	lineCount, err := s.fileLineCount(file)
	if err != nil {
		return fmt.Errorf("%s -> %w", op, err)
	}

	var res = make([]string, 0, lineCount)

	reader := bufio.NewReader(file)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("%s -> %w", op, err)
		}

		res = append(res, strings.TrimSpace(line))
	}

	s.storeData(ctx, res)

	s.log.LogAttrs(ctx, slog.LevelInfo, "Bytes count", slog.Int("bytes", s.size()))

	return nil
}

func (s *EmailService) storeData(ctx context.Context, payload []string) {
	slices.Sort(payload)

	var dataLen int

	for _, val := range payload {
		dataLen += len(val)
	}

	var data = make([]byte, 0, dataLen)
	var offset = make([]uint32, len(payload))

	for idx, val := range payload {
		offset[idx] = uint32(len(data))
		data = append(data, []byte(val)...)
	}

	s.storage.Store(&storage{
		data:    data,
		offsets: offset,
	})

	s.log.LogAttrs(ctx, slog.LevelInfo, "Bytes count", slog.Int("bytes", s.size()))
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

func (s *EmailService) resetCursor(file *os.File) error {
	const op = "emailservice.resetCursor"

	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("%s -> %w", op, err)
	}

	return nil
}

func (s *EmailService) fileLineCount(file *os.File) (count int, err error) {
	const op = "emailservice.fileLineCount"

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		count++
	}

	if err = s.resetCursor(file); err != nil {
		return 0, fmt.Errorf("%s -> %w", op, err)
	}

	return count, nil
}
