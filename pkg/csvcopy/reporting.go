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

	RowCount int64
}

func (r *Report) Rate() float64 {
	return float64(r.RowCount) / float64(r.Timestamp.Sub(r.StartedAt).Seconds())
}

func (r *Report) RateSince(previous Report) float64 {
	return float64(r.RowCount-previous.RowCount) / float64(r.Timestamp.Sub(previous.Timestamp).Seconds())
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
			"(%s), row rate %0.2f/sec (period), row rate %0.2f/sec (overall), %s total rows",
			formatDuration(totalTook),
			rowrate,
			overallRowrate,
			p.Sprintf("%d", r.RowCount),
		)
		previous = r
	}
}
