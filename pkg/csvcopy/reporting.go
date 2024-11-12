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

	RowCount int64
}

func (r *Report) Rate() float64 {
	return float64(r.RowCount) / float64(r.Timestamp.Sub(r.StartedAt).Seconds())
}

func (r *Report) RateSince(previous Report) float64 {
	return float64(r.RowCount-previous.RowCount) / float64(r.Timestamp.Sub(previous.Timestamp).Seconds())
}

func DefaultReportFunc(logger Logger) ReportFunc {
	previous := Report{
		StartedAt: time.Now(),
		Timestamp: time.Now(),
		RowCount:  0,
	}
	p := message.NewPrinter(language.English)

	return func(r Report) {
		rowrate := r.RateSince(previous)
		overallRowrate := r.Rate()
		totalTook := r.Timestamp.Sub(r.StartedAt)

		logger.Infof(
			"(%v), row rate %0.2f/sec (period), row rate %0.2f/sec (overall), %s total rows",
			totalTook-(totalTook%time.Second),
			rowrate,
			overallRowrate,
			p.Sprintf("%d", r.RowCount),
		)
		previous = r
	}
}
