package energy_sensors

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
	"time"
)

type AggregateForSensors struct {
	core      utils.QueryGenerator
	sensors   int
	duration  time.Duration
	aggregate string
}

func NewAggregateForSensors(sensors int, duration time.Duration, aggregate string) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &AggregateForSensors{
			core:      core,
			sensors:   sensors,
			duration:  duration,
			aggregate: aggregate,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *AggregateForSensors) Fill(q query.Query) query.Query {
	fc, ok := d.core.(AggregateFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.AggregateForSensors(q, d.sensors, d.duration, time.Minute*5, d.aggregate)
	return q
}
