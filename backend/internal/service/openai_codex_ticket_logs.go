package service

import (
	"container/list"
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

const OpenAICodexTicketLogLimit = 200
const openAICodexTicketLogMaxStreams = 512

var ErrOpenAICodexTicketLogModel = errors.New("model is not configured for Codex tickets")
var errOpenAICodexTicketHarvestPaused = errors.New("codex ticket harvest paused")

// Allow-listed diagnostics only: no upstream error text, bodies, tickets or credentials.
type OpenAICodexTicketLogEntry struct {
	ID           int64     `json:"id"`
	Time         time.Time `json:"time"`
	Attempt      uint64    `json:"attempt"`
	Event        string    `json:"event"`
	Reason       string    `json:"reason"`
	HTTPStatus   int       `json:"http_status,omitempty"`
	TicketLength int       `json:"ticket_length"`
	DurationMS   int64     `json:"duration_ms"`
}

type OpenAICodexTicketLogs struct {
	Model        string                      `json:"model"`
	Entries      []OpenAICodexTicketLogEntry `json:"entries"`
	Status       *OpenAICodexTicketStatus    `json:"status"`
	Limit        int                         `json:"limit"`
	TargetLength int                         `json:"target_length"`
}

type openAICodexTicketLogStream struct {
	key        string
	entries    [OpenAICodexTicketLogLimit]OpenAICodexTicketLogEntry
	next, size int
}

// Fixed-size per-stream rings and LRU bound memory even after account deletion.
type openAICodexTicketLogStore struct {
	mu      sync.Mutex
	streams map[string]*list.Element
	lru     list.List
	nextID  int64
}

func (store *openAICodexTicketLogStore) append(accountID int64, model string, entry OpenAICodexTicketLogEntry) {
	key := openAICodexTicketKey(accountID, normalizeOpenAICodexTicketModel(model))
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.streams == nil {
		store.streams = make(map[string]*list.Element)
	}
	e := store.streams[key]
	if e == nil {
		if len(store.streams) >= openAICodexTicketLogMaxStreams {
			old := store.lru.Back()
			delete(store.streams, old.Value.(*openAICodexTicketLogStream).key)
			store.lru.Remove(old)
		}
		e = store.lru.PushFront(&openAICodexTicketLogStream{key: key})
		store.streams[key] = e
	} else {
		store.lru.MoveToFront(e)
	}
	store.nextID++
	entry.ID, entry.Time = store.nextID, time.Now().UTC()
	stream := e.Value.(*openAICodexTicketLogStream)
	stream.entries[stream.next] = entry
	stream.next = (stream.next + 1) % OpenAICodexTicketLogLimit
	stream.size = min(stream.size+1, OpenAICodexTicketLogLimit)
}

func (store *openAICodexTicketLogStore) snapshot(accountID int64, model string) []OpenAICodexTicketLogEntry {
	store.mu.Lock()
	defer store.mu.Unlock()
	entries := make([]OpenAICodexTicketLogEntry, 0)
	e := store.streams[openAICodexTicketKey(accountID, normalizeOpenAICodexTicketModel(model))]
	if e == nil {
		return entries
	}
	store.lru.MoveToFront(e)
	stream := e.Value.(*openAICodexTicketLogStream)
	for i := 0; i < stream.size; i++ {
		entries = append(entries, stream.entries[(stream.next-stream.size+i+OpenAICodexTicketLogLimit)%OpenAICodexTicketLogLimit])
	}
	return entries
}

func (s *OpenAIGatewayService) OpenAICodexTicketLogs(ctx context.Context, account *Account, model string, now time.Time) (*OpenAICodexTicketLogs, error) {
	model = normalizeOpenAICodexTicketModel(model)
	cfg := s.openAICodexTicketConfig()
	configured := false
	for _, candidate := range cfg.Models {
		if normalizeOpenAICodexTicketModel(candidate) == model {
			configured = true
			break
		}
	}
	if !configured {
		return nil, ErrOpenAICodexTicketLogModel
	}
	result := &OpenAICodexTicketLogs{Model: model, Entries: []OpenAICodexTicketLogEntry{}, Limit: OpenAICodexTicketLogLimit, TargetLength: cfg.TargetLength}
	if s == nil || !isOpenAICodexTicketAccount(account) {
		return result, nil
	}
	result.Entries = s.openaiCodexTicketLogs.snapshot(account.ID, model)
	cfg.Enabled = s.openAICodexTicketEnabledContext(ctx)
	for _, status := range s.OpenAICodexTicketStatuses(account, cfg, now) {
		if status.Model == model {
			result.Status = &status
			break
		}
	}
	return result, nil
}

func codexTicketProbeErrorReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	return "request_error"
}
