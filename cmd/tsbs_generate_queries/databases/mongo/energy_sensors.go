package mongo

import (
	"encoding/gob"
	"fmt"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/energy_sensors"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/query"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

func init() {
	// needed for serializing the mongo query to gob
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
	gob.Register([]map[string]interface{}{})
	gob.Register(bson.M{})
	gob.Register(bson.D{})
	gob.Register([]bson.M{})
}

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

func (d *EnergySensors) getRandomSensors(nHosts int) []string {
	names, err := d.GetRandomSensors(nHosts)
	panicIfErr(err)
	return names
}

func (d *EnergySensors) LastPointForSensors(qi query.Query, nSensors int) {
	sensornames := d.getRandomSensors(nSensors)
	pipelineQuery := []bson.M{
		{
			"$match": bson.M{"tags.sensorname": bson.M{"$in": sensornames}},
		},
		{
			"$sort": bson.M{"time": -1},
		},
		{
			"$group": bson.M{"_id": "$tags.sensorname",
				"time":  bson.M{"$first": "$time"},
				"value": bson.M{"$first": "$value"},
			},
		},
	}
	/*
		db.point_data.aggregate( [
		    {"$match": {"tags.sensorname": {"$in": ["sensor_0", "sensor_11", "sensor_3"]}}},
		    { $sort : {time:-1}},
		    { $group : { _id : "$tags.sensorname",
		            time : {$first:"$time"},
		            value : {$first:"$value"}
		        }  }
		] )
	*/

	humanLabel := "Mongo last row for sensors"
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s", humanLabel))
}

func (d *EnergySensors) HistoryForSensors(qi query.Query, nSensors int, timeRange time.Duration) {
	interval := d.Interval.MustRandWindow(timeRange)
	sensornames := d.getRandomSensors(nSensors)

	pipelineQuery := []bson.M{
		{
			"$match": bson.M{"tags.sensorname": bson.M{"$in": sensornames}},
		},
		{
			"$match": bson.M{"time": bson.M{"$gte": interval.Start()}},
		},
		{
			"$match": bson.M{"time": bson.M{"$lt": interval.End()}},
		},
		{
			"$sort": bson.M{"time": 1},
		},
	}

	humanLabel := "Mongo history for sensors"
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s", humanLabel))
}

func (d *EnergySensors) AggregateForSensors(qi query.Query, nSensors int, timeRange time.Duration, aggInterval time.Duration, aggregate string) {
	humanLabel := "MongoDB " + aggregate + " aggregated history for sensors"

	interval := d.Interval.MustRandWindow(timeRange)
	sensornames := d.getRandomSensors(nSensors)
	if aggregate == energy_sensors.AggRand {
		aggregate = common.RandomStringSliceChoice(energy_sensors.AggChoices)
	}
	if aggregate == energy_sensors.AggStdDev {
		aggregate = "stdDevSamp"
	}
	if aggregate == energy_sensors.AggVariance {
		panic("not implemented")
	}

	aggVar := fmt.Sprintf("%s_value", aggregate)
	aggFunc := fmt.Sprintf("$%s", aggregate)
	aggObj := bson.M{aggFunc: "$value"}
	if aggregate == energy_sensors.AggCount {
		aggObj = bson.M{aggFunc: bson.M{}}
	}

	pipelineQuery := []bson.M{
		{
			"$match": bson.M{"tags.sensorname": bson.M{"$in": sensornames}},
		},
		{
			"$match": bson.M{"time": bson.M{"$gte": interval.Start()}},
		},
		{
			"$match": bson.M{"time": bson.M{"$lt": interval.End()}},
		},
		{
			"$addFields": bson.M{"tb": bson.M{"$add": []interface{}{
				bson.M{"$subtract": []interface{}{
					bson.M{"$subtract": []interface{}{"$time", time.Time{}}},
					bson.M{"$mod": []interface{}{
						bson.M{"$subtract": []interface{}{"$time", time.Time{}}},
						1000 * int(aggInterval.Seconds()),
					},
					},
				}},
				time.Time{},
			}}},
		},
		{
			"$group": bson.M{
				"_id": bson.M{
					"sensorname":  "$tags.sensorname",
					"time_bucket": "$tb",
				},
				aggVar: aggObj,
			},
		},
		{
			"$sort": bson.M{"time_bucket": 1},
		},
	}

	/*db.point_data.aggregate([
	    {"$match": {"tags.sensorname": {"$in": ["sensor_42", "sensor_76", "sensor_78", "sensor_44", "sensor_5", "sensor_31", "sensor_80", "sensor_6", "sensor_25", "sensor_19"]}}},
	    {"$match": {"time": {"$gte": ISODate("2020-01-01T05:12:38.404Z")}}},
	    {"$match": {"time": {"$lt": ISODate("2020-01-02T05:12:38.404Z")}}},
	    {
	        "$addFields": {
	            tb: {
	                "$add": [
	                    {
	                        "$subtract": [
	                            {"$subtract": ["$time", new Date(0)]},
	                            {
	                                "$mod": [
	                                    {"$subtract": ["$time", new Date(0)]},
	                                    1000 * 60 * 5
	                                ]
	                            }
	                        ]
	                    },
	                    new Date(0)
	                ]
	            },
	        }
	    },
	    {
	        "$group": {
	            _id: {
	                sensorname: "$tags.sensorname",
	                time_bucket: "$tb"
	            },
	            avg_value: {$avg: "$value"}
	        }
	    },
	    {
	        "$project": {
	            sensorname: "$_id.sensorname",
	            time_bucket: "$_id.time_bucket",
	            _id: 0,
	            avg_value: 1
	        }
	    },
	    {$sort: {time_bucket: 1}}
	])
	*/
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s", humanLabel))
}

func (d *EnergySensors) ThresholdFilterForSensors(qi query.Query, nSensors int, timeRange time.Duration, lower int, upper int) {
	interval := d.Interval.MustRandWindow(timeRange)
	sensornames := d.getRandomSensors(nSensors)

	pipelineQuery := []bson.M{
		{
			"$match": bson.M{"tags.sensorname": bson.M{"$in": sensornames}},
		},
		{
			"$match": bson.M{"time": bson.M{"$gte": interval.Start()}},
		},
		{
			"$match": bson.M{"time": bson.M{"$lt": interval.End()}},
		},
		{
			"$match": bson.M{"$or": []bson.M{
				{"value": bson.M{"$lt": lower}},
				{"value": bson.M{"$gt": upper}},
			},
			},
		},
		{
			"$sort": bson.M{"time": 1},
		},
	}

	/*
		db.point_data.aggregate( [
		    {"$match": {"tags.sensorname": {"$in": ["sensor_0", "sensor_11", "sensor_3"]}}},
		    {"$match": {"time": {"$gte": ISODate("2020-01-04T00:00:00.000Z")}}},
		    {"$match": {"time": {"$lt": ISODate("2020-01-05T00:00:00.000Z")}}},
		    {"$match": {"$or": [{"value": {"$lt": 3}}, {"value": {"$gt": 6}}]}},
		    { $sort : {time:1}},
		] )
	*/

	humanLabel := "Mongo threshold filter for sensors"
	q := qi.(*query.Mongo)
	q.HumanLabel = []byte(humanLabel)
	q.BsonDoc = pipelineQuery
	q.CollectionName = []byte("point_data")
	q.HumanDescription = []byte(fmt.Sprintf("%s", humanLabel))
}
