package batcher

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/google/uuid"
	"serv/internal/backend"
	"serv/internal/metrics"
	"serv/internal/models"
)

// BatchResult holds the inference outcome for an individual client.
type BatchResult struct {
	Response *models.ChatCompletionResponse
	Err      error
}

// BatchItem encapsulates a single incoming request and its return channel.
type BatchItem struct {
	ID         string
	Request    *models.ChatCompletionRequest
	ResultChan chan *BatchResult
	Context    context.Context
	EnqueuedAt time.Time
}

// DynamicBatcher aggregates individual LLM inference requests into coalesced batches.
type DynamicBatcher struct {
	client        *backend.Client
	inQueue       chan *BatchItem
	maxBatchSize  int
	batchTimeout  time.Duration
	maxConcurrent int
	sem           chan struct{}
	modelName     string
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

// NewDynamicBatcher constructs and starts the batch collection loop.
func NewDynamicBatcher(client *backend.Client, maxBatchSize int, batchTimeout time.Duration, queueCapacity int, maxConcurrent int, defaultModel string) *DynamicBatcher {
	if maxConcurrent <= 0 {
		maxConcurrent = 8
	}

	ctx, cancel := context.WithCancel(context.Background())
	b := &DynamicBatcher{
		client:        client,
		inQueue:       make(chan *BatchItem, queueCapacity),
		maxBatchSize:  maxBatchSize,
		batchTimeout:  batchTimeout,
		maxConcurrent: maxConcurrent,
		sem:           make(chan struct{}, maxConcurrent),
		modelName:     defaultModel,
		ctx:           ctx,
		cancel:        cancel,
	}

	b.wg.Add(1)
	go b.batchLoop()

	return b
}

// Submit enqueues a request into the batcher and blocks until completion or context cancellation.
func (b *DynamicBatcher) Submit(ctx context.Context, req *models.ChatCompletionRequest) (*models.ChatCompletionResponse, error) {
	reqID := uuid.New().String()
	item := &BatchItem{
		ID:         reqID,
		Request:    req,
		ResultChan: make(chan *BatchResult, 1),
		Context:    ctx,
		EnqueuedAt: time.Now(),
	}

	metrics.QueueDepth.Inc()
	defer metrics.QueueDepth.Dec()

	select {
	case <-b.ctx.Done():
		return nil, fmt.Errorf("batcher is shutting down")
	case <-ctx.Done():
		return nil, ctx.Err()
	case b.inQueue <- item:
	}

	// Wait for batch processing outcome or client abort
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-item.ResultChan:
		if res.Err != nil {
			return nil, res.Err
		}
		return res.Response, nil
	}
}

// batchLoop continuously accumulates items until maxBatchSize is reached or batchTimeout expires.
func (b *DynamicBatcher) batchLoop() {
	defer b.wg.Done()

	currentBatch := make([]*BatchItem, 0, b.maxBatchSize)
	timer := time.NewTimer(b.batchTimeout)
	timerRunning := true

	flush := func() {
		if len(currentBatch) == 0 {
			return
		}
		toDispatch := currentBatch
		currentBatch = make([]*BatchItem, 0, b.maxBatchSize)

		// Acquire concurrency semaphore
		b.sem <- struct{}{}
		metrics.ActiveInferences.Inc()

		go func(items []*BatchItem) {
			defer func() {
				<-b.sem
				metrics.ActiveInferences.Dec()
			}()
			b.processBatch(items)
		}(toDispatch)
	}

	for {
		select {
		case <-b.ctx.Done():
			// Flush pending items on shutdown
			if len(currentBatch) > 0 {
				flush()
			}
			return

		case item := <-b.inQueue:
			// Prune cancelled requests before batching
			if item.Context.Err() != nil {
				continue
			}

			currentBatch = append(currentBatch, item)

			// If this is the first item in a new batch, reset timer
			if len(currentBatch) == 1 && !timerRunning {
				timer.Reset(b.batchTimeout)
				timerRunning = true
			}

			// If batch reached maximum capacity, dispatch immediately
			if len(currentBatch) >= b.maxBatchSize {
				if !timer.Stop() && timerRunning {
					select {
					case <-timer.C:
					default:
					}
				}
				timerRunning = false
				flush()
			}

		case <-timer.C:
			timerRunning = false
			if len(currentBatch) > 0 {
				flush()
			}
		}
	}
}

type batchGroupKey struct {
	model       string
	temperature float64
	topP        float64
}

// processBatch partitions coalesced items by compatible model/sampling parameters
// and dispatches them to the quantized backend.
func (b *DynamicBatcher) processBatch(items []*BatchItem) {
	// Filter out items whose client context was cancelled while waiting for worker dispatch
	groups := make(map[batchGroupKey][]*BatchItem)
	order := make([]batchGroupKey, 0, 1)

	for _, item := range items {
		if item.Context.Err() != nil {
			continue
		}
		model := b.modelName
		if item.Request.Model != "" {
			model = item.Request.Model
		}
		key := batchGroupKey{
			model:       model,
			temperature: item.Request.Temperature,
			topP:        item.Request.TopP,
		}
		if _, exists := groups[key]; !exists {
			order = append(order, key)
		}
		groups[key] = append(groups[key], item)
	}

	if len(order) == 0 {
		return
	}

	if len(order) == 1 {
		k := order[0]
		b.executeGroup(k, groups[k])
		return
	}

	var wg sync.WaitGroup
	for _, k := range order {
		wg.Add(1)
		go func(key batchGroupKey, grp []*BatchItem) {
			defer wg.Done()
			b.executeGroup(key, grp)
		}(k, groups[k])
	}
	wg.Wait()
}

func (b *DynamicBatcher) executeGroup(key batchGroupKey, validItems []*BatchItem) {
	start := time.Now()
	metrics.BatchSize.Observe(float64(len(validItems)))

	backendItems := make([]backend.BatchInferenceItem, 0, len(validItems))
	for _, item := range validItems {
		maxTokens := item.Request.MaxTokens
		if maxTokens <= 0 {
			maxTokens = 256
		}
		backendItems = append(backendItems, backend.BatchInferenceItem{
			ID:        item.ID,
			Messages:  item.Request.Messages,
			MaxTokens: maxTokens,
		})
	}

	batchReq := &backend.BatchInferenceRequest{
		Model:       key.model,
		Items:       backendItems,
		Temperature: key.temperature,
		TopP:        key.topP,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	batchResp, err := b.client.ExecuteBatch(ctx, batchReq)
	duration := time.Since(start)
	metrics.BatchProcessingDuration.Observe(duration.Seconds())

	if err != nil {
		log.Printf("[Batcher] Batch of size %d failed: %v", len(validItems), err)
		for _, item := range validItems {
			if item.Context.Err() == nil {
				item.ResultChan <- &BatchResult{
					Err: fmt.Errorf("backend batch error: %w", err),
				}
			}
		}
		return
	}

	// Map results back to client channels by request ID
	resultMap := make(map[string]backend.BatchInferenceItemResponse, len(batchResp.Results))
	for _, res := range batchResp.Results {
		resultMap[res.ID] = res
	}

	now := time.Now().Unix()
	for _, item := range validItems {
		if item.Context.Err() != nil {
			continue
		}

		res, found := resultMap[item.ID]
		if !found {
			item.ResultChan <- &BatchResult{
				Err: fmt.Errorf("missing result for item %s in batch response", item.ID),
			}
			continue
		}

		if res.Error != "" {
			item.ResultChan <- &BatchResult{
				Err: fmt.Errorf("inference worker error: %s", res.Error),
			}
			continue
		}

		completionResp := &models.ChatCompletionResponse{
			ID:                 res.ID,
			Object:             "chat.completion",
			Created:            now,
			Model:              key.model,
			Choices:            []models.ChatChoice{res.Choice},
			Usage:              res.Usage,
			Cached:             false,
			LatencyMs:          time.Since(item.EnqueuedAt).Milliseconds(),
			TimeToFirstTokenMs: res.TimeToFirstToken,
		}

		item.ResultChan <- &BatchResult{
			Response: completionResp,
		}
	}
}

// Stop stops the batcher and waits for pending batches to complete.
func (b *DynamicBatcher) Stop() {
	b.cancel()
	b.wg.Wait()
}
