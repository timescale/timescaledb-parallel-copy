package csvcopy

import (
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type ReportFunc func(Report)

type Report struct {
	Timestamp time.Time
	StartedAt time.Time

	InsertedRows int64
	TotalRows    int64
}

func (r *Report) Rate() float64 {
	return float64(r.InsertedRows) / float64(r.Timestamp.Sub(r.StartedAt).Seconds())
}

func (r *Report) RateSince(previous Report) float64 {
	return float64(r.InsertedRows-previous.InsertedRows) / float64(r.Timestamp.Sub(previous.Timestamp).Seconds())
}

func DefaultReportFunc(logger Logger) ReportFunc {
	previous := Report{
		StartedAt:    time.Now(),
		Timestamp:    time.Now(),
		InsertedRows: 0,
		TotalRows:    0,
	}
	p := message.NewPrinter(language.English)

	return func(r Report) {
		rowrate := r.RateSince(previous)
		overallRowrate := r.Rate()
		totalTook := r.Timestamp.Sub(r.StartedAt)

		logger.Infof(
			"(%v), row rate %0.2f/sec (period), row rate %0.2f/sec (overall), %s total inserted rows, %s total rows",
			totalTook-(totalTook%time.Second),
			rowrate,
			overallRowrate,
			p.Sprintf("%d", r.InsertedRows),
			p.Sprintf("%d", r.TotalRows),
		)
		previous = r
	}
}
