package energy_sensors

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
	"time"
)

type HistoryForSensors struct {
	core     utils.QueryGenerator
	sensors  int
	duration time.Duration
}

func NewHistoryForSensors(sensors int, duration time.Duration) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &HistoryForSensors{
			core:     core,
			sensors:  sensors,
			duration: duration,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *HistoryForSensors) Fill(q query.Query) query.Query {
	fc, ok := d.core.(HistoryFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.HistoryForSensors(q, d.sensors, d.duration)
	return q
}
