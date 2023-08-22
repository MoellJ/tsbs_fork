package energy_sensors

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/pkg/query"
	"time"
)

const (
	LastPointSingleSensor    = "lastpoint"
	LastPointMultipleSensors = "lastpoint-multiple-sensors"
	SmallHistory             = "history"
	LargeHistory             = "history-large"
	Aggregate                = "aggregate-"

	AggRand     = "rand"
	AggAvg      = "avg"
	AggSum      = "sum"
	AggMax      = "max"
	AggMin      = "min"
	AggCount    = "count"
	AggStdDev   = "StdDev"
	AggVariance = "variance"
)

var (
	AggChoices = []string{
		AggAvg,
		AggSum,
		AggMax,
		AggMin,
		AggCount,
		AggStdDev,
		AggVariance,
	}
)

type Core struct {
	*common.Core
}

// NewCore returns a new Core for the given time range and cardinality
func NewCore(start, end time.Time, scale int) (*Core, error) {
	c, err := common.NewCore(start, end, scale)
	return &Core{Core: c}, err

}

type LastPointFiller interface {
	LastPointForSensors(query.Query, int)
}

type HistoryFiller interface {
	HistoryForSensors(query.Query, int, time.Duration)
}

type AggregateFiller interface {
	AggregateForSensors(query.Query, int, time.Duration, time.Duration, string)
}

func (d *Core) GetRandomSensors(nSensors int) ([]string, error) {
	return getRandomSensors(nSensors, d.Scale)
}

func getRandomSensors(numSensors int, totalSensors int) ([]string, error) {
	if numSensors < 1 {
		return nil, fmt.Errorf("number of sensors cannot be < 1; got %d", numSensors)
	}
	if numSensors > totalSensors {
		return nil, fmt.Errorf("number of sensors (%d) larger than total sensors. See --scale (%d)", numSensors, totalSensors)
	}

	randomNumbers, err := common.GetRandomSubsetPerm(numSensors, totalSensors)
	if err != nil {
		return nil, err
	}

	sensors := []string{}
	for _, n := range randomNumbers {
		sensors = append(sensors, fmt.Sprintf("sensor_%d", n))
	}

	return sensors, nil
}
