package pullcontrol

import (
	"container/heap"
	"context"
	"sync"

	"golang.org/x/sync/semaphore"
)

// Priority levels
const (
	Low = iota
	Normal
	High
)

type priorityKey struct{}
type imageRefKey struct{}

var (
	PriorityKey = priorityKey{}
	ImageRefKey = imageRefKey{}
)

type request struct {
	priority int
	ctx      context.Context
	ready    chan struct{}
}

type requestQueue []*request

func (pq requestQueue) Len() int            { return len(pq) }
func (pq requestQueue) Less(i, j int) bool  { return pq[i].priority > pq[j].priority }
func (pq requestQueue) Swap(i, j int)       { pq[i], pq[j] = pq[j], pq[i] }
func (pq *requestQueue) Push(x interface{}) { *pq = append(*pq, x.(*request)) }
func (pq *requestQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*pq = old[0 : n-1]
	return item
}

type Controller struct {
	mu           sync.Mutex
	queue        requestQueue
	pausedImages map[string]struct{}
	pauseCond    *sync.Cond
	limiter      *semaphore.Weighted
	processing   bool

	activePulls  map[string]map[int]int // imageRef -> priority -> count
	priorityCond *sync.Cond
}

func (c *Controller) Acquire(ctx context.Context) (func(), error) {
	readyChan := make(chan struct{})
	priority := Normal
	if p, ok := ctx.Value(PriorityKey).(int); ok {
		priority = p
	}

	req := &request{
		priority: priority,
		ctx:      ctx,
		ready:    readyChan,
	}

	c.mu.Lock()
	heap.Push(&c.queue, req)
	c.maybeStartProcessor()
	c.mu.Unlock()

	select {
	case <-readyChan:
		imageRef, _ := ctx.Value(ImageRefKey).(string)
		if imageRef != "" {
			c.mu.Lock()
			for {
				if _, paused := c.pausedImages[imageRef]; !paused {
					break
				}
				c.pauseCond.Wait()
			}

			// Wait for higher priority images to complete all layers
			for !c.canProceedWithPriority(imageRef, priority) {
				c.priorityCond.Wait()
			}

			// Track this pull
			c.trackPull(imageRef, priority)
			c.mu.Unlock()
		}

		if err := c.limiter.Acquire(ctx, 1); err != nil {
			// Untrack the pull if semaphore acquisition failed
			if imageRef != "" {
				c.mu.Lock()
				c.untrackPull(imageRef, priority)
				c.mu.Unlock()
			}
			c.mu.Lock()
			c.maybeStartProcessor()
			c.mu.Unlock()
			return nil, err
		}

		releaseFunc := func() {
			c.limiter.Release(1)
			c.mu.Lock()
			if imageRef != "" {
				c.untrackPull(imageRef, priority)
			}
			c.maybeStartProcessor()
			c.mu.Unlock()
		}
		return releaseFunc, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Controller) maybeStartProcessor() {
	if c.processing || c.queue.Len() == 0 {
		return
	}
	c.processing = true
	go func() {
		c.mu.Lock()
		if c.queue.Len() == 0 {
			c.processing = false
			c.mu.Unlock()
			return
		}
		req := heap.Pop(&c.queue).(*request)
		c.processing = false
		c.mu.Unlock()

		if req.ctx.Err() != nil {
			c.mu.Lock()
			c.maybeStartProcessor()
			c.mu.Unlock()
			return
		}

		close(req.ready)
	}()
}

func (c *Controller) Pause(imageRef string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pausedImages[imageRef] = struct{}{}
}

func (c *Controller) Resume(imageRef string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pausedImages, imageRef)
	c.pauseCond.Broadcast()
}

func (c *Controller) canProceedWithPriority(imageRef string, requestPriority int) bool {
	if requestPriority == High {
		return true
	}

	// Check if any higher priority images have active pulls
	for otherImageRef, priorities := range c.activePulls {
		if otherImageRef == imageRef {
			continue // Skip self
		}
		for priority, count := range priorities {
			if priority > requestPriority && count > 0 {
				return false // Higher priority image is still pulling
			}
		}
	}
	return true
}

func (c *Controller) trackPull(imageRef string, priority int) {
	if c.activePulls == nil {
		c.activePulls = make(map[string]map[int]int)
	}
	if c.activePulls[imageRef] == nil {
		c.activePulls[imageRef] = make(map[int]int)
	}
	c.activePulls[imageRef][priority]++
}

func (c *Controller) untrackPull(imageRef string, priority int) {
	if c.activePulls == nil || c.activePulls[imageRef] == nil {
		return
	}

	c.activePulls[imageRef][priority]--
	if c.activePulls[imageRef][priority] <= 0 {
		delete(c.activePulls[imageRef], priority)
		if len(c.activePulls[imageRef]) == 0 {
			delete(c.activePulls, imageRef)
		}
	}

	c.priorityCond.Broadcast()
}

var GlobalController = &Controller{
	pausedImages: make(map[string]struct{}),
}

func Init(limiter *semaphore.Weighted) {
	GlobalController.limiter = limiter
	GlobalController.pauseCond = sync.NewCond(&GlobalController.mu)
	GlobalController.priorityCond = sync.NewCond(&GlobalController.mu)
	GlobalController.activePulls = make(map[string]map[int]int)
}
