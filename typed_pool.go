package websockets

import "sync"

// TypedPool is a generic wrapper around sync.Pool that provides type safety.
type TypedPool[T any] struct {
	pool sync.Pool
}

// NewTypedPool initializes a pool with a generic factory function.
func NewTypedPool[T any](factory func() *T) *TypedPool[T] {
	return &TypedPool[T]{
		pool: sync.Pool{
			New: func() any {
				return factory()
			},
		},
	}
}

// Get retrieves a strongly-typed item from the pool.
func (p *TypedPool[T]) Get() *T {
	return p.pool.Get().(*T)
}

// Put returns a strongly-typed item to the pool.
func (p *TypedPool[T]) Put(v *T) {
	p.pool.Put(v)
}
