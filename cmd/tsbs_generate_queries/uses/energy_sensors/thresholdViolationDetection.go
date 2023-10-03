package energy_sensors

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
	"time"
)

type ThresholdFilterForSensors struct {
	core     utils.QueryGenerator
	sensors  int
	duration time.Duration
	lower    int
	upper    int
}

func NewThresholdFilterForSensors(sensors int, duration time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &ThresholdFilterForSensors{
			core:     core,
			sensors:  sensors,
			duration: duration,
			lower:    6,
			upper:    20,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *ThresholdFilterForSensors) Fill(q query.Query) query.Query {
	fc, ok := d.core.(ThresholdFilterFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.ThresholdFilterForSensors(q, d.sensors, d.duration, d.lower, d.upper)
	return q
}
