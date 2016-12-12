package mdata

import (
	"time"

	"github.com/raintank/metrictank/mdata/chunk"
)

type ChunkReadRequest struct {
	month     uint32
	sortKey   uint32
	q         string
	p         []interface{}
	timestamp time.Time
	out       chan outcome
}

type ChunkWriteRequest struct {
	key       string
	chunk     *chunk.Chunk
	ttl       uint32
	timestamp time.Time
	len       uint32
}
