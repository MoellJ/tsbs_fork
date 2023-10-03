package timescaledb

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/energy_sensors"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/query"
	"strings"
	"time"
)

type EnergySensors struct {
	*energy_sensors.Core
	*BaseGenerator
}

func NewEnergySensors(start, end time.Time, scale int, g *BaseGenerator) *EnergySensors {
	c, err := energy_sensors.NewCore(start, end, scale)
	panicIfErr(err)
	return &EnergySensors{
		Core:          c,
		BaseGenerator: g,
	}
}

func (d *EnergySensors) getSensorsWhereString(nHosts int) string {
	names, err := d.GetRandomSensors(nHosts)
	panicIfErr(err)
	var nameClauses []string
	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("'%s'", s))
	}
	return fmt.Sprintf("sensorname IN (%s)", strings.Join(nameClauses, ","))
}

func (d *EnergySensors) LastPointForSensors(qi query.Query, nSensors int) {
	var sql string
	sql = fmt.Sprintf(`SELECT DISTINCT ON (sensorname) * FROM readings WHERE %s ORDER BY sensorname, time DESC`,
		d.getSensorsWhereString(nSensors))
	humanLabel := "TimescaleDB last row for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, "readings", sql)
}

func (d *EnergySensors) HistoryForSensors(qi query.Query, nSensors int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	var sql string
	sql = fmt.Sprintf(`SELECT * FROM readings WHERE %s and time >= '%s' and time < '%s' ORDER BY time ASC`,
		d.getSensorsWhereString(nSensors),
		interval.StartString(),
		interval.EndString())
	humanLabel := "TimescaleDB history for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, "readings", sql)
}

func (d *EnergySensors) AggregateForSensors(qi query.Query, nSensors int, timeRange time.Duration, aggInterval time.Duration, aggregate string) {
	interval := d.Interval.MustRandWindow(timeRange)
	if aggregate == energy_sensors.AggRand {
		aggregate = common.RandomStringSliceChoice(energy_sensors.AggChoices)
	}
	aggClause := fmt.Sprintf("%[1]s(value) as %[1]s_value", aggregate)
	var sql string
	sql = fmt.Sprintf(`SELECT time_bucket('%s seconds', time) as timeframe, sensorname, %s FROM readings 
                            WHERE %s and time >= '%s' and time < '%s'
                            GROUP BY timeframe, sensorname ORDER BY timeframe`,
		fmt.Sprintf("%f", aggInterval.Seconds()),
		aggClause,
		d.getSensorsWhereString(nSensors),
		interval.StartString(),
		interval.EndString())

	humanLabel := "TimescaleDB aggregated history for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, "readings", sql)
}

func (d *EnergySensors) ThresholdFilterForSensors(qi query.Query, nSensors int, timeRange time.Duration, lower int, upper int) {
	interval := d.Interval.MustRandWindow(timeRange)
	var sql string
	sql = fmt.Sprintf(`SELECT * FROM readings WHERE %s and time >= '%s' and time < '%s' and (value < %d or value > %d) ORDER BY time ASC`,
		d.getSensorsWhereString(nSensors),
		interval.StartString(),
		interval.EndString(),
		lower, upper)
	humanLabel := "TimescaleDB threshold filter for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, "readings", sql)
}
