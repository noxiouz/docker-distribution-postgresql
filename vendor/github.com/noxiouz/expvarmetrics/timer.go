package expvarmetrics

import (
	"expvar"
	"time"

	"github.com/rcrowley/go-metrics"
)

var (
	_ expvar.Var = TimerVar{}
)

// TimerVar adds expvar.Var interface to go-metrics.Timer
type TimerVar struct {
	metrics.Timer
}

// NewTimerVar returns new TimerVar with go-metrics.StandartTimer inside
func NewTimerVar() TimerVar {
	return TimerVar{
		Timer: metrics.NewTimer(),
	}
}

type timerStats struct {
	Sum        int64           `json:"sum"`
	Min        int64           `json:"min"`
	Max        int64           `json:"max"`
	Mean       float64         `json:"mean"`
	Rate       rateStats       `json:"rate"`
	Percentile percentileStats `json:"percentile"`
}

func (t TimerVar) String() string {
	ss := t.Snapshot()
	percentiles := ss.Percentiles(requestedPercentiles)
	norm := int64(time.Millisecond)
	normf := float64(norm)
	var stat = timerStats{
		Sum:  ss.Sum() / norm,
		Min:  ss.Min() / norm,
		Max:  ss.Max() / norm,
		Mean: ss.Mean() / normf,
		Rate: rateStats{
			Rate1:    ss.Rate1(),
			Rate5:    ss.Rate5(),
			Rate15:   ss.Rate15(),
			RateMean: ss.RateMean(),
		},
		Percentile: percentileStats{
			Q50:   percentiles[0] / normf,
			Q75:   percentiles[1] / normf,
			Q90:   percentiles[2] / normf,
			Q95:   percentiles[3] / normf,
			Q98:   percentiles[4] / normf,
			Q99:   percentiles[5] / normf,
			Q9995: percentiles[6] / normf,
		},
	}

	return toString(&stat)
}
