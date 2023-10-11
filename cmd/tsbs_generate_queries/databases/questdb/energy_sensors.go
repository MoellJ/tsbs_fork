package questdb

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
	sql = fmt.Sprintf(`SELECT * FROM readings latest BY sensorname WHERE %s`,
		d.getSensorsWhereString(nSensors))
	humanLabel := "QuestDB last row for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

func (d *EnergySensors) HistoryForSensors(qi query.Query, nSensors int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	var sql string
	sql = fmt.Sprintf(`SELECT * FROM readings WHERE %s and timestamp >= '%s' and timestamp < '%s' ORDER BY timestamp ASC`,
		d.getSensorsWhereString(nSensors),
		interval.StartString(),
		interval.EndString())
	humanLabel := "QuestDB history for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

func (d *EnergySensors) AggregateForSensors(qi query.Query, nSensors int, timeRange time.Duration, aggInterval time.Duration, aggregate string) {
	humanLabel := "QuestDB " + aggregate + " aggregated history for sensors"
	humanDesc := humanLabel

	interval := d.Interval.MustRandWindow(timeRange)
	if aggregate == energy_sensors.AggRand {
		aggregate = common.RandomStringSliceChoice(energy_sensors.AggChoices)
	}
	if aggregate == energy_sensors.AggStdDev {
		aggregate = "stddev_samp"
	}
	if aggregate == energy_sensors.AggVariance {
		panic("not implemented")
	}

	aggClause := fmt.Sprintf("%[1]s(value) as %[1]s_value", aggregate)
	var sql string
	sql = fmt.Sprintf(`SELECT timestamp, sensorname, %s FROM readings 
                            WHERE %s and timestamp >= '%s' and timestamp < '%s'
                            SAMPLE BY %ds`,
		aggClause,
		d.getSensorsWhereString(nSensors),
		interval.StartString(),
		interval.EndString(),
		int(aggInterval.Seconds()))

	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

func (d *EnergySensors) ThresholdFilterForSensors(qi query.Query, nSensors int, timeRange time.Duration, lower int, upper int) {
	interval := d.Interval.MustRandWindow(timeRange)
	var sql string
	sql = fmt.Sprintf(`SELECT * FROM readings WHERE %s and timestamp >= '%s' and timestamp < '%s' and (value < %d or value > %d) ORDER BY timestamp ASC`,
		d.getSensorsWhereString(nSensors),
		interval.StartString(),
		interval.EndString(),
		lower,
		upper)
	humanLabel := "QuestDB threshold filter for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}
