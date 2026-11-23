package storage

import (
	"context"
	"log"
	"sync"
	"time"

	"serv/internal/models"
)

// AsyncAuditWriter buffers request logs in memory and writes them in batches to PostgreSQL.
type AsyncAuditWriter struct {
	store         *PostgresStore
	recordChan    chan models.RequestMetadata
	flushInterval time.Duration
	batchSize     int
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewAsyncAuditWriter starts the background worker routine.
func NewAsyncAuditWriter(store *PostgresStore, bufferCap, batchSize int, flushInterval time.Duration) *AsyncAuditWriter {
	ctx, cancel := context.WithCancel(context.Background())
	w := &AsyncAuditWriter{
		store:         store,
		recordChan:    make(chan models.RequestMetadata, bufferCap),
		flushInterval: flushInterval,
		batchSize:     batchSize,
		ctx:           ctx,
		cancel:        cancel,
	}

	w.wg.Add(1)
	go w.worker()

	return w
}

// Enqueue puts a record into the buffered channel without blocking the HTTP handler.
func (w *AsyncAuditWriter) Enqueue(rec models.RequestMetadata) {
	select {
	case w.recordChan <- rec:
	default:
		// Queue full - drop or log to prevent memory exhaustion
		log.Printf("[Storage] Audit buffer full! Dropping request log for %s", rec.RequestID)
	}
}

// worker flushes records periodically or when batch size threshold is reached.
func (w *AsyncAuditWriter) worker() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	batch := make([]models.RequestMetadata, 0, w.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if w.store == nil {
			batch = make([]models.RequestMetadata, 0, w.batchSize)
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := w.store.InsertBatch(ctx, batch); err != nil {
			log.Printf("[Storage] Async flush failed for %d records: %v", len(batch), err)
		}
		batch = make([]models.RequestMetadata, 0, w.batchSize)
	}

	for {
		select {
		case <-w.ctx.Done():
			// Drain remaining records before exit
			for {
				select {
				case rec := <-w.recordChan:
					batch = append(batch, rec)
					if len(batch) >= w.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}

		case rec := <-w.recordChan:
			batch = append(batch, rec)
			if len(batch) >= w.batchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

// Stop flushes all pending records and closes gracefully.
func (w *AsyncAuditWriter) Stop() {
	w.cancel()
	w.wg.Wait()
}
