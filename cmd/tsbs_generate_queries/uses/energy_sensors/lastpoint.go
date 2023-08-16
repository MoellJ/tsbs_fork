package energy_sensors

import (
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/common"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

type LastPointForSensors struct {
	core    utils.QueryGenerator
	sensors int
}

func NewLastPointForSensors(sensors int) utils.QueryFillerMaker {
	return func(core utils.QueryGenerator) utils.QueryFiller {
		return &LastPointForSensors{
			core:    core,
			sensors: sensors,
		}
	}
}

// Fill fills in the query.Query with query details
func (d *LastPointForSensors) Fill(q query.Query) query.Query {
	fc, ok := d.core.(LastPointFiller)
	if !ok {
		common.PanicUnimplementedQuery(d.core)
	}
	fc.LastPointForSensors(q, d.sensors)
	return q
}
