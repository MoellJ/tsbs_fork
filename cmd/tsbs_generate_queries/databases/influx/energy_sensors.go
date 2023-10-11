package influx

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/databases"
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
	databases.PanicIfErr(err)
	return &EnergySensors{
		Core:          c,
		BaseGenerator: g,
	}
}

func (d *EnergySensors) getSensorsWhereString(nSensors int) string {
	names, err := d.GetRandomSensors(nSensors)
	databases.PanicIfErr(err)
	nameClauses := []string{}
	for _, s := range names {
		nameClauses = append(nameClauses, fmt.Sprintf("sensorname = '%s'", s))
	}

	combinedNameClause := strings.Join(nameClauses, " or ")
	return "(" + combinedNameClause + ")"
}

func (d *EnergySensors) LastPointForSensors(qi query.Query, nSensors int) {
	var sql string
	sql = fmt.Sprintf(`SELECT * from readings WHERE %s group by "sensorname" order by time desc limit 1`,
		d.getSensorsWhereString(nSensors))
	humanLabel := "Influx last row for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

func (d *EnergySensors) HistoryForSensors(qi query.Query, nSensors int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	var sql string
	sql = fmt.Sprintf(`SELECT * FROM readings WHERE %s and time >= '%s' and time < '%s' ORDER BY time ASC`,
		d.getSensorsWhereString(nSensors),
		interval.StartString(),
		interval.EndString())
	humanLabel := "Influx history for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}

func (d *EnergySensors) AggregateForSensors(qi query.Query, nSensors int, timeRange time.Duration, aggInterval time.Duration, aggregate string) {
	humanLabel := "Influx " + aggregate + " aggregated history for sensors"
	humanDesc := humanLabel

	interval := d.Interval.MustRandWindow(timeRange)
	if aggregate == energy_sensors.AggRand {
		aggregate = common.RandomStringSliceChoice(energy_sensors.AggChoices)
	}
	if aggregate == energy_sensors.AggAvg {
		aggregate = "mean"
	}
	if aggregate == energy_sensors.AggVariance {
		panic("not implemented")
	}
	aggClause := fmt.Sprintf("%s(value)", aggregate)
	var sql string
	sql = fmt.Sprintf(`SELECT %s FROM readings 
                            WHERE %s and time >= '%s' and time < '%s'
                            group by time(%ds), "sensorname"`,
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
	sql = fmt.Sprintf(`SELECT * FROM readings WHERE %s and time >= '%s' and time < '%s' and (value < %d or value > %d) ORDER BY time ASC`,
		d.getSensorsWhereString(nSensors),
		interval.StartString(),
		interval.EndString(),
		lower,
		upper)
	humanLabel := "Influx threshold filter for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, sql)
}
