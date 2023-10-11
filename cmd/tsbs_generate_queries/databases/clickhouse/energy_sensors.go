package clickhouse

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
	panicIfErr(err)
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
	if d.UseTags {
		sql = fmt.Sprintf(`SELECT * FROM 
		(
			SELECT * FROM readings WHERE (tags_id, time) IN 
			(
				SELECT tags_id, max(time) FROM readings GROUP BY tags_id
			)
		) AS r INNER JOIN tags AS t ON r.tags_id = t.id WHERE %s`,
			d.getSensorsWhereString(nSensors))
	} else {

		sql = fmt.Sprintf(`SELECT * FROM readings WHERE %s AND (sensorname, created_at) IN
			  (
				  SELECT sensorname, max(created_at) FROM readings GROUP BY sensorname
			  )`,
			d.getSensorsWhereString(nSensors))
	}
	humanLabel := "ClickHouse last row for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, "readings", sql)
}

func (d *EnergySensors) HistoryForSensors(qi query.Query, nSensors int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	var sql string
	if d.UseTags {
		sql = fmt.Sprintf(`SELECT *
		FROM 
		(
			SELECT * FROM readings 
			WHERE (created_at >= toDateTime('%s')) AND (created_at < toDateTime('%s'))
		) AS r INNER JOIN tags AS t ON r.tags_id = t.id WHERE %s`,
			interval.Start().Format(clickhouseTimeStringFormat),
			interval.End().Format(clickhouseTimeStringFormat),
			d.getSensorsWhereString(nSensors))
	} else {
		sql = fmt.Sprintf(`SELECT * FROM readings
			WHERE %s AND (created_at >= toDateTime('%s')) AND (created_at < toDateTime('%s'))`,
			d.getSensorsWhereString(nSensors),
			interval.Start().Format(clickhouseTimeStringFormat),
			interval.End().Format(clickhouseTimeStringFormat))
	}
	humanLabel := "ClickHouse history for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, "readings", sql)
}

func (d *EnergySensors) AggregateForSensors(qi query.Query, nSensors int, timeRange time.Duration, aggInterval time.Duration, aggregate string) {
	interval := d.Interval.MustRandWindow(timeRange)
	if aggregate == energy_sensors.AggRand {
		aggregate = common.RandomStringSliceChoice(energy_sensors.AggChoices)
	}
	if aggregate == energy_sensors.AggStdDev {
		aggregate = "stddevPopStable"
	}
	if aggregate == energy_sensors.AggVariance {
		aggregate = "stddevPop"
	}
	aggClause := fmt.Sprintf("%[1]s(value)", aggregate)
	var sql string

	if d.UseTags {
		panic("not implemented") //TODO
	} else {
		sql = fmt.Sprintf(`SELECT sensorname, toStartOfInterval(created_at, toIntervalSecond(%d)) AS timeinterval, %s
			FROM readings
			WHERE %s AND (created_at >= toDateTime('%s')) AND (created_at < toDateTime('%s')) GROUP BY timeinterval, sensorname`,
			int(aggInterval.Seconds()),
			aggClause,
			d.getSensorsWhereString(nSensors),
			interval.Start().Format(clickhouseTimeStringFormat),
			interval.End().Format(clickhouseTimeStringFormat))
	}

	humanLabel := "ClickHouse aggregated history for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, "readings", sql)
}

func (d *EnergySensors) ThresholdFilterForSensors(qi query.Query, nSensors int, timeRange time.Duration, lower int, upper int) {
	interval := d.Interval.MustRandWindow(timeRange)
	var sql string
	if d.UseTags {
		sql = fmt.Sprintf(`SELECT *
		FROM 
		(
			SELECT * FROM readings 
			WHERE (created_at >= toDateTime('%s')) AND (created_at < toDateTime('%s'))
			AND (value < %d OR value > %d)
		) AS r INNER JOIN tags AS t ON r.tags_id = t.id WHERE %s`,
			interval.Start().Format(clickhouseTimeStringFormat),
			interval.End().Format(clickhouseTimeStringFormat),
			lower,
			upper,
			d.getSensorsWhereString(nSensors))
	} else {
		sql = fmt.Sprintf(`SELECT * FROM readings
			WHERE %s AND (created_at >= toDateTime('%s')) AND (created_at < toDateTime('%s'))
			AND (value < %d OR value > %d)`,
			d.getSensorsWhereString(nSensors),
			interval.Start().Format(clickhouseTimeStringFormat),
			interval.End().Format(clickhouseTimeStringFormat),
			lower,
			upper)
	}
	humanLabel := "ClickHouse threshold filter for sensors"
	humanDesc := humanLabel
	d.fillInQuery(qi, humanLabel, humanDesc, "readings", sql)
}
