// tsbs_generate_queries generates queries for various use cases. Its output will
// be consumed by the corresponding tsbs_run_queries_ program.
package main

import (
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/energy_sensors"
	"github.com/timescale/tsbs/pkg/query/config"
	"os"
	"time"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/internal/inputs"
	internalUtils "github.com/timescale/tsbs/internal/utils"
)

var useCaseMatrix = map[string]map[string]utils.QueryFillerMaker{
	"devops": {
		devops.LabelSingleGroupby + "-1-1-1":  devops.NewSingleGroupby(1, 1, 1),
		devops.LabelSingleGroupby + "-1-1-12": devops.NewSingleGroupby(1, 1, 12),
		devops.LabelSingleGroupby + "-1-8-1":  devops.NewSingleGroupby(1, 8, 1),
		devops.LabelSingleGroupby + "-5-1-1":  devops.NewSingleGroupby(5, 1, 1),
		devops.LabelSingleGroupby + "-5-1-12": devops.NewSingleGroupby(5, 1, 12),
		devops.LabelSingleGroupby + "-5-8-1":  devops.NewSingleGroupby(5, 8, 1),
		devops.LabelMaxAll + "-1":             devops.NewMaxAllCPU(1, devops.MaxAllDuration),
		devops.LabelMaxAll + "-8":             devops.NewMaxAllCPU(8, devops.MaxAllDuration),
		devops.LabelMaxAll + "-32-24":         devops.NewMaxAllCPU(32, 24*time.Hour),
		devops.LabelDoubleGroupby + "-1":      devops.NewGroupBy(1),
		devops.LabelDoubleGroupby + "-5":      devops.NewGroupBy(5),
		devops.LabelDoubleGroupby + "-all":    devops.NewGroupBy(devops.GetCPUMetricsLen()),
		devops.LabelGroupbyOrderbyLimit:       devops.NewGroupByOrderByLimit,
		devops.LabelHighCPU + "-all":          devops.NewHighCPU(0),
		devops.LabelHighCPU + "-1":            devops.NewHighCPU(1),
		devops.LabelLastpoint:                 devops.NewLastPointPerHost,
	},
	"iot": {
		iot.LabelLastLoc:                       iot.NewLastLocPerTruck,
		iot.LabelLastLocSingleTruck:            iot.NewLastLocSingleTruck,
		iot.LabelLowFuel:                       iot.NewTruckWithLowFuel,
		iot.LabelHighLoad:                      iot.NewTruckWithHighLoad,
		iot.LabelStationaryTrucks:              iot.NewStationaryTrucks,
		iot.LabelLongDrivingSessions:           iot.NewTrucksWithLongDrivingSession,
		iot.LabelLongDailySessions:             iot.NewTruckWithLongDailySession,
		iot.LabelAvgVsProjectedFuelConsumption: iot.NewAvgVsProjectedFuelConsumption,
		iot.LabelAvgDailyDrivingDuration:       iot.NewAvgDailyDrivingDuration,
		iot.LabelAvgDailyDrivingSession:        iot.NewAvgDailyDrivingSession,
		iot.LabelAvgLoad:                       iot.NewAvgLoad,
		iot.LabelDailyActivity:                 iot.NewDailyTruckActivity,
		iot.LabelBreakdownFrequency:            iot.NewTruckBreakdownFrequency,
	},
	"energy-sensors": {
		energy_sensors.LastPointSingleSensor:                         energy_sensors.NewLastPointForSensors(1),
		energy_sensors.LastPointMultipleSensors:                      energy_sensors.NewLastPointForSensors(10),
		energy_sensors.SmallHistory:                                  energy_sensors.NewHistoryForSensors(1, time.Hour*24),
		energy_sensors.LargeHistory:                                  energy_sensors.NewHistoryForSensors(20, time.Hour*24*7),
		energy_sensors.Aggregate + energy_sensors.AggRand:            energy_sensors.NewAggregateForSensors(10, time.Hour*24, energy_sensors.AggRand),
		energy_sensors.Aggregate + energy_sensors.AggRand + "-large": energy_sensors.NewAggregateForSensors(50, time.Hour*24, energy_sensors.AggRand),
		energy_sensors.Aggregate + energy_sensors.AggAvg:             energy_sensors.NewAggregateForSensors(10, time.Hour*24, energy_sensors.AggAvg),
		energy_sensors.Aggregate + energy_sensors.AggMax:             energy_sensors.NewAggregateForSensors(10, time.Hour*24, energy_sensors.AggMax),
		energy_sensors.Aggregate + energy_sensors.AggMin:             energy_sensors.NewAggregateForSensors(10, time.Hour*24, energy_sensors.AggMin),
		energy_sensors.Aggregate + energy_sensors.AggSum:             energy_sensors.NewAggregateForSensors(10, time.Hour*24, energy_sensors.AggSum),
		energy_sensors.Aggregate + energy_sensors.AggCount:           energy_sensors.NewAggregateForSensors(10, time.Hour*24, energy_sensors.AggCount),
		energy_sensors.Aggregate + energy_sensors.AggStdDev:          energy_sensors.NewAggregateForSensors(10, time.Hour*24, energy_sensors.AggStdDev),
		energy_sensors.Aggregate + energy_sensors.AggVariance:        energy_sensors.NewAggregateForSensors(10, time.Hour*24, energy_sensors.AggVariance),
	},
}

var conf = &config.QueryGeneratorConfig{}

// Parse args:
func init() {
	useCaseMatrix["cpu-only"] = useCaseMatrix["devops"]
	// Change the Usage function to print the use case matrix of choices:
	oldUsage := pflag.Usage
	pflag.Usage = func() {
		oldUsage()

		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "The use case matrix of choices is:\n")
		for uc, queryTypes := range useCaseMatrix {
			for qt := range queryTypes {
				fmt.Fprintf(os.Stderr, "  use case: %s, query type: %s\n", uc, qt)
			}
		}
	}

	conf.AddToFlagSet(pflag.CommandLine)

	pflag.Parse()

	err := internalUtils.SetupConfigFile()

	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}

	if err := viper.Unmarshal(&conf.BaseConfig); err != nil {
		panic(fmt.Errorf("unable to decode base config: %s", err))
	}

	if err := viper.Unmarshal(&conf); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}
}

func main() {
	qg := inputs.NewQueryGenerator(useCaseMatrix)
	err := qg.Generate(conf)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
}
