package expvarmetrics

import (
	"bytes"
	"encoding/json"
)

type rateStats struct {
	Rate1    float64 `json:"1min"`
	Rate5    float64 `json:"5min"`
	Rate15   float64 `json:"15min"`
	RateMean float64 `json:"mean"`
}

type percentileStats struct {
	Q50   float64 `json:"50%"`
	Q75   float64 `json:"75%"`
	Q90   float64 `json:"90%"`
	Q95   float64 `json:"95%"`
	Q98   float64 `json:"98%"`
	Q99   float64 `json:"99%"`
	Q9995 float64 `json:"99.95%"`
}

func toString(stats interface{}) string {
	buff := new(bytes.Buffer)
	json.NewEncoder(buff).Encode(stats)
	return buff.String()
}
