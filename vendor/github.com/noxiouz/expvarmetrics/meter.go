package expvarmetrics

import (
	"expvar"

	"github.com/rcrowley/go-metrics"
)

var (
	requestedPercentiles = []float64{0.5, 0.75, 0.9, 0.95, 0.98, 0.99, 0.9995}

	_ expvar.Var = &MeterVar{}
)

// MeterVar adds expvar.Var interface to go-metrics.Meter
type MeterVar struct {
	metrics.Meter
}

// NewMeterVar returns new MeterVar with go-metrics.StandartMeter inside
func NewMeterVar() MeterVar {
	return MeterVar{
		Meter: metrics.NewMeter(),
	}
}

type meterStats struct {
	Rate  rateStats `json:"rate"`
	Count int64     `json:"count"`
}

func (m MeterVar) String() string {
	ss := m.Snapshot()
	stats := meterStats{
		Count: ss.Count(),
		Rate: rateStats{
			Rate1:    ss.Rate1(),
			Rate5:    ss.Rate5(),
			Rate15:   ss.Rate15(),
			RateMean: ss.RateMean(),
		},
	}

	return toString(&stats)
}
