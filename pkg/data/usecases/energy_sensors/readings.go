package energy_sensors

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"math/rand"
	"time"
)

var (
	labelReadings = []byte("readings")
	tempStepUD    = common.UD(-0.005, 0.005)

	smallUD = common.UD(-1, 1)

	temperatureFields = []common.LabeledDistributionMaker{
		{
			Label: []byte("value"),
			DistributionMaker: func() common.Distribution {
				return common.FP(
					common.CWD(tempStepUD, 5.0, 35.0, rand.Float64()*25.0+5),
					5,
				)
			},
		},
	}

	powerFields = []common.LabeledDistributionMaker{
		{
			Label: []byte("value"),
			DistributionMaker: func() common.Distribution {
				return common.FP(
					common.CWD(smallUD, 0, 15.0, rand.Float64()*15.0),
					5,
				)
			},
		},
	}
)

// ReadingsMeasurement represents a subset of truck measurement readings.
type ReadingsMeasurement struct {
	*common.SubsystemMeasurement
}

// ToPoint serializes ReadingsMeasurement to serialize.Point.
func (m *ReadingsMeasurement) ToPoint(p *data.Point) {
	p.SetMeasurementName(labelReadings)
	copy := m.Timestamp
	p.SetTimestamp(&copy)

	for _, d := range m.Distributions {
		p.AppendField([]byte("value"), float64(d.Get()))
	}
}

// NewReadingsMeasurement creates a new ReadingsMeasurement with start time.
func NewReadingsMeasurement(start time.Time, unit string) *ReadingsMeasurement {
	var sub = common.NewSubsystemMeasurementWithDistributionMakers(start, temperatureFields)
	switch unit {
	case "°C":
		sub = common.NewSubsystemMeasurementWithDistributionMakers(start, temperatureFields)
	case "kW":
		sub = common.NewSubsystemMeasurementWithDistributionMakers(start, powerFields)
	default:
		sub = common.NewSubsystemMeasurementWithDistributionMakers(start, temperatureFields)
	}

	return &ReadingsMeasurement{
		SubsystemMeasurement: sub,
	}
}
