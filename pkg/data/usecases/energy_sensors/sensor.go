package energy_sensors

import (
	"fmt"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"time"
)

const (
	sensorNameFmt = "sensor_%d"
)

var (
	DataFormatChoices = []string{
		"Continuous_Numeric",
		"Generic_Numeric",
		"Discrete_Numeric",
	}

	UnitChoices = []string{
		"temp2",
		"temp3",
		"°C",
		"kW",
	}
)

// Sensor models a sensor which sends back measurements.
type Sensor struct {
	simulatedMeasurements []common.SimulatedMeasurement
	tags                  []common.Tag
}

// TickAll advances all Distributions of a Sensor.
func (t *Sensor) TickAll(d time.Duration) {
	for i := range t.simulatedMeasurements {
		t.simulatedMeasurements[i].Tick(d)
	}
}

// Measurements returns the sensors measurements.
func (t Sensor) Measurements() []common.SimulatedMeasurement {
	return t.simulatedMeasurements
}

// Tags returns the sensor tags.
func (t Sensor) Tags() []common.Tag {
	return t.tags
}

func newSensorMeasurements(start time.Time, unit string) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		NewReadingsMeasurement(start, unit),
	}
}

// NewSensor creates a new sensor in a simulated energy sensor use case
func NewSensor(i int, start time.Time) common.Generator {
	sensor := newSensorWithMeasurementGenerator(i, start, newSensorMeasurements)
	return &sensor
}

func newSensorWithMeasurementGenerator(i int, start time.Time, generator func(time.Time, string) []common.SimulatedMeasurement) Sensor {
	unit := common.RandomStringSliceChoice(UnitChoices)
	sm := generator(start, unit)

	h := Sensor{
		tags: []common.Tag{
			{Key: []byte("sensorname"), Value: fmt.Sprintf(sensorNameFmt, i)},
		},
		//			{Key: []byte("data_format"), Value: common.RandomStringSliceChoice(DataFormatChoices)},
		//			{Key: []byte("unit"), Value: unit},
		simulatedMeasurements: sm,
	}

	return h
}
