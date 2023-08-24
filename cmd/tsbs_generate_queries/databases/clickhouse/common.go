package clickhouse

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/energy_sensors"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// BaseGenerator contains settings specific for ClickHouse.
type BaseGenerator struct {
	UseTags bool
}

// GenerateEmptyQuery returns an empty query.ClickHouse.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewClickHouse()
}

// fill Query fills the query struct with data
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, table, sql string) {
	q := qi.(*query.ClickHouse)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.Table = []byte(table)
	q.SqlQuery = []byte(sql)
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	devops := &Devops{
		BaseGenerator: g,
		Core:          core,
	}

	return devops, nil
}

// NewIoT creates a new iot use case query generator.
func (g *BaseGenerator) NewIoT(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := iot.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	iot := &IoT{
		BaseGenerator: g,
		Core:          core,
	}

	return iot, nil
}

// NewEnergySensors creates a new energy sensors use case query generator.
func (g *BaseGenerator) NewEnergySensors(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := energy_sensors.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	energy_sensors := &EnergySensors{
		BaseGenerator: g,
		Core:          core,
	}

	return energy_sensors, nil
}
