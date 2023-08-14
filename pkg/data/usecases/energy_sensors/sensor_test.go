package energy_sensors

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"testing"
	"time"
)

func testGenerator(s time.Time, unit string) []common.SimulatedMeasurement {
	return []common.SimulatedMeasurement{
		&testMeasurement{ticks: 0},
	}
}

type testMeasurement struct {
	ticks int
}

func (m *testMeasurement) Tick(_ time.Duration)  { m.ticks++ }
func (m *testMeasurement) ToPoint(_ *data.Point) {}

func TestNewSensorMeasurements(t *testing.T) {
	start := time.Now()

	measurements := newSensorMeasurements(start, "°C")

	if got := len(measurements); got != 1 {
		t.Errorf("incorrect number of measurements: got %d want %d", got, 1)
	}

	// Cast each measurement to its type; will panic if wrong types
	readings := measurements[0].(*ReadingsMeasurement)
	if got := readings.Timestamp; got != start {
		t.Errorf("incorrect readings measurement timestamp: got %v want %v", got, start)
	}
}

func TestNewSensor(t *testing.T) {
	start := time.Now()
	generator := NewSensor(1, start)

	sensor := generator.(*Sensor)

	if got := len(sensor.Measurements()); got != 1 {
		t.Errorf("incorrect sensor measurement count: got %v want %v", got, 1)
	}

	if got := len(sensor.Tags()); got != 3 {
		t.Errorf("incorrect sensor tag count: got %v want %v", got, 3)
	}
}

func TestSensorTickAll(t *testing.T) {
	now := time.Now()
	sensor := newSensorWithMeasurementGenerator(0, now, testGenerator)
	if got := sensor.simulatedMeasurements[0].(*testMeasurement).ticks; got != 0 {
		t.Errorf("ticks not equal to 0 to start: got %d", got)
	}
	sensor.TickAll(time.Second)
	if got := sensor.simulatedMeasurements[0].(*testMeasurement).ticks; got != 1 {
		t.Errorf("ticks incorrect: got %d want %d", got, 1)
	}
	sensor.simulatedMeasurements = append(sensor.simulatedMeasurements, &testMeasurement{})
	sensor.TickAll(time.Second)
	if got := sensor.simulatedMeasurements[0].(*testMeasurement).ticks; got != 2 {
		t.Errorf("ticks incorrect after 2nd tick: got %d want %d", got, 2)
	}
	if got := sensor.simulatedMeasurements[1].(*testMeasurement).ticks; got != 1 {
		t.Errorf("ticks incorrect after 2nd tick: got %d want %d", got, 1)
	}
}
