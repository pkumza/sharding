package sharding

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"
)

const (
	defaultEpsNum    = 7000
	defaultInstNum   = 7000
	defaultShardSize = 100
	testLoop         = 200
)

func Test_Sqrt(t *testing.T) {
	testSharding(AlgSqrt)
}
func Test_Const(t *testing.T) {
	testSharding(AlgConst)
}
func Test_Linear(t *testing.T) {
	testSharding(AlgLinear)
}
func Test_Mixer(t *testing.T) {
	testSharding(AlgMixer)
}

func testSharding(alg Algorithm) {
	rand.Seed(time.Now().UnixNano())
	loadRange := make(map[int]int)
	for l := 0; l < testLoop; l++ {
		endpoints := make([]string, defaultEpsNum)
		epsLoad := make(map[string]float64)
		for i := 0; i < defaultEpsNum; i++ {
			epName := genEndpoint()
			endpoints[i] = epName
			epsLoad[epName] = 0.0
		}
		s := New(alg, defaultShardSize, endpoints)
		for i := 0; i < defaultInstNum; i++ {
			eps := s.Get(genInstName())
			for _, ep := range eps {
				epsLoad[ep] += 1.0 / float64(len(eps))
			}
		}
		for _, load := range epsLoad {
			loadRange[int(math.Round(load/0.01))]++
		}
	}
	output, err := os.Create(fmt.Sprintf("slice_%v.out", alg))
	if err != nil {
		panic(err)
	}
	defer output.Close()
	loadMax := 0
	for load := range loadRange {
		if load > loadMax {
			loadMax = load
		}
	}
	for i := 0; i <= loadMax; i++ {
		if loadRange[i] == 0 {
			output.WriteString("\n")
		} else {
			output.WriteString(strconv.Itoa(loadRange[i]) + "\n")
		}
	}
}

func genEndpoint() string {
	return fmt.Sprintf("10.%d.%d.%d:%d", rand.Intn(255), rand.Intn(255), rand.Intn(255), rand.Intn(65536))
}

func genInstName() string {
	return fmt.Sprintf("dp-%s-%s-%s",
		randString(10), randString(10), randString(5))
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

// RandString - A helper function create and fill a slice of length n with characters from a-zA-Z0-9_-.
func randString(n int) string {
	output := make([]byte, n)

	// We will take n bytes, one byte for each character of output.
	randomness := make([]byte, n)

	// read all random
	_, err := rand.Read(randomness)
	if err != nil {
		panic(err)
	}

	l := len(letterBytes)
	// fill output
	for pos := range output {
		// get random item
		random := uint8(randomness[pos])

		// random % 64
		randomPos := random % uint8(l)

		// put into output
		output[pos] = letterBytes[randomPos]
	}

	return string(output)
}
