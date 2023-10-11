package query

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/spf13/pflag"
	"golang.org/x/time/rate"
)

const (
	labelAllQueries  = "all queries"
	labelColdQueries = "cold queries"
	labelWarmQueries = "warm queries"

	defaultReadSize = 4 << 20 // 4 MB
)

// BenchmarkRunnerConfig is the configuration of the benchmark runner.
type BenchmarkRunnerConfig struct {
	DBName           string `mapstructure:"db-name"`
	Limit            uint64 `mapstructure:"max-queries"`
	LimitRPS         uint64 `mapstructure:"max-rps"`
	MemProfile       string `mapstructure:"memprofile"`
	HDRLatenciesFile string `mapstructure:"hdr-latencies"`
	Workers          uint   `mapstructure:"workers"`
	PrintResponses   bool   `mapstructure:"print-responses"`
	Debug            int    `mapstructure:"debug"`
	FileName         string `mapstructure:"file"`
	BurnIn           uint64 `mapstructure:"burn-in"`
	Cooldown         uint64 `mapstructure:"cooldown"`
	PrintInterval    uint64 `mapstructure:"print-interval"`
	PrewarmQueries   bool   `mapstructure:"prewarm-queries"`
	ResultsFile      string `mapstructure:"results-file"`
}

// AddToFlagSet adds command line flags needed by the BenchmarkRunnerConfig to the flag set.
func (c BenchmarkRunnerConfig) AddToFlagSet(fs *pflag.FlagSet) {
	fs.String("db-name", "benchmark", "Name of database to use for queries")
	fs.Uint64("burn-in", 0, "Number of queries to ignore before collecting statistics.")
	fs.Uint64("cooldown", 0, "Number of queries to ignore at the end of run. Can only be used when 'max-queries' is set.")
	fs.Uint64("max-queries", 0, "Limit the number of queries to send, 0 = no limit")
	fs.Uint64("max-rps", 0, "Limit the rate of queries per second, 0 = no limit")
	fs.Uint64("print-interval", 100, "Print timing stats to stderr after this many queries (0 to disable)")
	fs.String("memprofile", "", "Write a memory profile to this file.")
	fs.String("hdr-latencies", "", "Write the High Dynamic Range (HDR) Histogram of Response Latencies to this file.")
	fs.Uint("workers", 1, "Number of concurrent requests to make.")
	fs.Bool("prewarm-queries", false, "Run each query twice in a row so the warm query is guaranteed to be a cache hit")
	fs.Bool("print-responses", false, "Pretty print response bodies for correctness checking (default false).")
	fs.Int("debug", 0, "Whether to print debug messages.")
	fs.String("file", "", "File name to read queries from")
	fs.String("results-file", "", "Write the test results summary json to this file")
}

// BenchmarkRunner contains the common components for running a query benchmarking
// program against a database.
type BenchmarkRunner struct {
	BenchmarkRunnerConfig
	br      *bufio.Reader
	sp      statProcessor
	scanner *scanner
	ch      chan Query
}

// NewBenchmarkRunner creates a new instance of BenchmarkRunner which is
// common functionality to be used by query benchmarker programs
func NewBenchmarkRunner(config BenchmarkRunnerConfig) *BenchmarkRunner {
	runner := &BenchmarkRunner{BenchmarkRunnerConfig: config}
	runner.scanner = newScanner(&runner.Limit)
	spArgs := &statProcessorArgs{
		limit:            &runner.Limit,
		printInterval:    runner.PrintInterval,
		prewarmQueries:   runner.PrewarmQueries,
		burnIn:           runner.BurnIn,
		cooldown:         runner.Cooldown,
		hdrLatenciesFile: runner.HDRLatenciesFile,
	}

	runner.sp = newStatProcessor(spArgs)
	return runner
}

// SetLimit changes the number of queries to run, with 0 being all of them
func (b *BenchmarkRunner) SetLimit(limit uint64) {
	b.Limit = limit
}

// DoPrintResponses indicates whether responses for queries should be printed
func (b *BenchmarkRunner) DoPrintResponses() bool {
	return b.PrintResponses
}

// DebugLevel returns the level of debug messages for this benchmark
func (b *BenchmarkRunner) DebugLevel() int {
	return b.Debug
}

// DatabaseName returns the name of the database to run queries against
func (b *BenchmarkRunner) DatabaseName() string {
	return b.DBName
}

// ProcessorCreate is a function that creates a new Processor (called in Run)
type ProcessorCreate func() Processor

// Processor is an interface that handles the setup of a query processing worker and executes queries one at a time
type Processor interface {
	// Init initializes at global state for the Processor, possibly based on its worker number / ID
	Init(workerNum int)

	// ProcessQuery handles a given query and reports its stats
	ProcessQuery(q Query, isWarm bool) ([]*Stat, error)
}

// GetBufferedReader returns the buffered Reader that should be used by the loader
func (b *BenchmarkRunner) GetBufferedReader() *bufio.Reader {
	if b.br == nil {
		if len(b.FileName) > 0 {
			// Read from specified file
			file, err := os.Open(b.FileName)
			if err != nil {
				panic(fmt.Sprintf("cannot open file for read %s: %v", b.FileName, err))
			}
			b.br = bufio.NewReaderSize(file, defaultReadSize)
		} else {
			// Read from STDIN
			b.br = bufio.NewReaderSize(os.Stdin, defaultReadSize)
		}
	}
	return b.br
}

// Run does the bulk of the benchmark execution.
// It launches a gorountine to track stats, creates workers to process queries,
// read in the input, execute the queries, and then does cleanup.
func (b *BenchmarkRunner) Run(queryPool *sync.Pool, processorCreateFn ProcessorCreate) {
	if b.Workers == 0 {
		panic("must have at least one worker")
	}

	spArgs := b.sp.getArgs()
	if spArgs.cooldown > 0 && b.Limit == 0 {
		panic("Cooldown can only be set when 'max-queries' is known")
	}
	if spArgs.burnIn+spArgs.cooldown > b.Limit {
		panic("burn-in + cooldown is larger than limit")
	}
	b.ch = make(chan Query, b.Workers)

	// Launch the stats processor:
	go b.sp.process(b.Workers)

	rateLimiter := getRateLimiter(b.LimitRPS, b.Workers)

	// Launch query processors
	var wg sync.WaitGroup
	for i := 0; i < int(b.Workers); i++ {
		wg.Add(1)
		go b.processorHandler(&wg, rateLimiter, queryPool, processorCreateFn(), i)
	}

	// Read in jobs, closing the job channel when done:
	// Wall clock start time
	wallStart := time.Now()
	b.scanner.setReader(b.GetBufferedReader()).scan(queryPool, b.ch)
	close(b.ch)

	// Block for workers to finish sending requests, closing the stats channel when done:
	wg.Wait()
	b.sp.CloseAndWait()

	// Wall clock end time
	wallEnd := time.Now()
	wallTook := wallEnd.Sub(wallStart)
	_, err := fmt.Printf("wall clock time: %fsec\n", float64(wallTook.Nanoseconds())/1e9)
	if err != nil {
		log.Fatal(err)
	}

	// (Optional) create a memory profile:
	if len(b.MemProfile) > 0 {
		f, err := os.Create(b.MemProfile)
		if err != nil {
			log.Fatal(err)
		}
		pprof.WriteHeapProfile(f)
		f.Close()
	}

	// (Optional) save the results file:
	if len(b.BenchmarkRunnerConfig.ResultsFile) > 0 {
		b.saveTestResult(wallTook, wallStart, wallEnd)
	}
}

func (b *BenchmarkRunner) saveTestResult(took time.Duration, start time.Time, end time.Time) {
	testResult := LoaderTestResult{
		ResultFormatVersion: BenchmarkTestResultVersion,
		RunnerConfig:        b.BenchmarkRunnerConfig,
		StartTime:           start.UTC().Unix() * 1000,
		EndTime:             end.UTC().Unix() * 1000,
		DurationMillis:      took.Milliseconds(),
		Totals:              b.sp.GetTotalsMap(),
	}

	_, _ = fmt.Printf("Saving results json file to %s\n", b.BenchmarkRunnerConfig.ResultsFile)
	file, err := json.MarshalIndent(testResult, "", " ")
	if err != nil {
		fmt.Printf("%#v", testResult)
		log.Fatal(err)
	}

	err = ioutil.WriteFile(b.BenchmarkRunnerConfig.ResultsFile, file, 0644)
	if err != nil {
		log.Fatal(err)
	}
}

func (b *BenchmarkRunner) processorHandler(wg *sync.WaitGroup, rateLimiter *rate.Limiter, queryPool *sync.Pool, processor Processor, workerNum int) {
	processor.Init(workerNum)
	for query := range b.ch {
		r := rateLimiter.Reserve()
		time.Sleep(r.Delay())

		stats, err := processor.ProcessQuery(query, false)
		if err != nil {
			panic(err)
		}
		b.sp.send(stats)

		// If PrewarmQueries is set, we run the query as 'cold' first (see above),
		// then we immediately run it a second time and report that as the 'warm' stat.
		// This guarantees that the warm stat will reflect optimal cache performance.
		spArgs := b.sp.getArgs()
		if spArgs.prewarmQueries {
			// Warm run
			stats, err = processor.ProcessQuery(query, true)
			if err != nil {
				panic(err)
			}
			b.sp.sendWarm(stats)
		}
		queryPool.Put(query)
	}
	wg.Done()
}

func getRateLimiter(limitRPS uint64, workers uint) *rate.Limiter {
	var requestRate = rate.Inf
	var requestBurst = 0
	if limitRPS != 0 {
		requestRate = rate.Limit(limitRPS)
		requestBurst = int(workers)
	}
	return rate.NewLimiter(requestRate, requestBurst)
}
