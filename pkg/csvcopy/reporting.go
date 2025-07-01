package csvcopy

import (
	"fmt"
	"time"

	"golang.org/x/text/language"
	"golang.org/x/text/message"
)

type ReportFunc func(Report)

type Report struct {
	Timestamp time.Time
	StartedAt time.Time

	// Rows inserted into the database by this copier instance
	InsertedRows int64
	// Rows skipped because they were already processed
	SkippedRows int64
	// Total rows read from source
	TotalRows int64
}

func (r *Report) Rate() float64 {
	return float64(r.InsertedRows) / float64(r.Timestamp.Sub(r.StartedAt).Seconds())
}

func (r *Report) RateSince(previous Report) float64 {
	return float64(r.InsertedRows-previous.InsertedRows) / float64(r.Timestamp.Sub(previous.Timestamp).Seconds())
}

func formatDuration(dur time.Duration) string {
	dur = dur.Truncate(time.Second)
	d := dur / (24 * time.Hour)
	dur -= d * (24 * time.Hour)
	h := dur / time.Hour
	dur -= h * time.Hour
	m := dur / time.Minute
	dur -= m * time.Minute
	s := dur / time.Second

	if d > 0 {
		return fmt.Sprintf("%dd%02dh%02dm%02ds", d, h, m, s)
	}
	if h > 0 {
		return fmt.Sprintf("%dh%02dm%02ds", h, m, s)
	}
	if m > 0 {
		return fmt.Sprintf("%dm%02ds", m, s)
	}
	return fmt.Sprintf("%ds", s)
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
			"(%s), row rate %0.2f/sec (period), row rate %0.2f/sec (overall), %s inserted rows, %s skipped rows, %s total rows",
			formatDuration(totalTook),
			rowrate,
			overallRowrate,
			p.Sprintf("%d", r.InsertedRows),
			p.Sprintf("%d", r.SkippedRows),
			p.Sprintf("%d", r.TotalRows),
		)
		previous = r
	}
}
