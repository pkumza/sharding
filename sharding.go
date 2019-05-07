package sharding

import (
	"math"

	"github.com/pkumza/consistent"
)

// Algorithm of sharding
type Algorithm int

const (
	// AlgSqrt Sqrt Weight
	AlgSqrt Algorithm = iota
	// AlgConst Const Weight
	AlgConst
	// AlgLinear Linear Weight
	AlgLinear
	// AlgMixer Const & Linear Weight
	AlgMixer
)

func (a Algorithm) String() string {
	switch a {
	case AlgSqrt:
		return "sqrt"
	case AlgConst:
		return "const"
	case AlgLinear:
		return "linear"
	case AlgMixer:
		return "mixer"
	default:
		return "unknown"
	}
}

const (
	NumOfReplicas = 100
)

// Sharding :
type Sharding struct {
	consistEps  *consistent.Consistent
	consistInst *consistent.Consistent
	shards      map[string][]string
}

// New creates a new Sharding object.
func New(alg Algorithm, shardSize int, endpoints []string) *Sharding {
	// Create a new Sharding
	s := &Sharding{
		consistEps:  consistent.New(),
		consistInst: consistent.New(),
		shards:      make(map[string][]string),
	}

	// Init Buckets(Shards)
	shardNum := len(endpoints) / shardSize
	for i := 0; i < shardNum; i++ {
		shardName := genShardName(i)
		s.consistEps.Add(shardName, NumOfReplicas)
		s.shards[shardName] = make([]string, 0)
	}
	s.consistEps.SortHashes()

	// Put Endpoints to Buckets
	for _, endpoint := range endpoints {
		shardName, _ := s.consistEps.Get(endpoint)
		s.shards[shardName] = append(s.shards[shardName], endpoint)
	}

	// Set up instance consistent
	for shardName, endpoints := range s.shards {
		switch alg {
		case AlgConst:
			s.consistInst.Add(shardName, NumOfReplicas)
		case AlgLinear:
			s.consistInst.Add(shardName, len(endpoints))
		case AlgSqrt:
			sqrt := int(math.Sqrt(float64(len(endpoints) * NumOfReplicas)))
			s.consistInst.Add(shardName, sqrt)
		case AlgMixer:
			if len(endpoints) > NumOfReplicas {
				s.consistInst.Add(shardName, NumOfReplicas)
			} else {
				s.consistInst.Add(shardName, len(endpoints))
			}
		default:
			panic("unknown algorithm")
		}
	}
	s.consistInst.SortHashes()
	return s
}

// Get returns a list of endpoints
func (s *Sharding) Get(instName string) (endpoints []string) {
	shardName, _ := s.consistInst.Get(instName)
	return s.shards[shardName]
}

// Get returns a list of endpoints
func (s *Sharding) GetTwo(instName string) (endpoints []string, endpointsTwo []string) {
	shardName, shardNameTwo, _ := s.consistInst.GetTwo(instName)
	return s.shards[shardName], s.shards[shardNameTwo]
}

var (
	alphaBeta = []byte("abcdefghijklmnopqrstuvwxyz")
)

func genShardName(idx int) (shardName string) {
	if idx >= 676 {
		panic("idx is too large for shard name")
	}
	shardName += string(alphaBeta[idx/26])
	shardName += string(alphaBeta[idx%26])
	return
}
