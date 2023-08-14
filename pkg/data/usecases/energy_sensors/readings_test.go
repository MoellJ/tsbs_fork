package energy_sensors

import (
	"github.com/timescale/tsbs/pkg/data"
	"testing"
	"time"
)

func TestReadingsMeasurementToPoint(t *testing.T) {
	now := time.Now()
	m := NewReadingsMeasurement(now, "°C")
	duration := time.Second
	m.Tick(duration)

	p := data.NewPoint()
	m.ToPoint(p)
	if got := string(p.MeasurementName()); got != string(labelReadings) {
		t.Errorf("incorrect measurement name: got %s want %s", got, labelReadings)
	}
}
