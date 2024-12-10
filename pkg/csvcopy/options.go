package csvcopy

type Option func(c *Copier)

type Logger interface {
	Infof(msg string, args ...interface{})
}

type noopLogger struct{}

func (l *noopLogger) Infof(msg string, args ...interface{}) {}

// WithLogger sets the logger where the application will print debug messages
func WithLogger(logger Logger) Option {
	return func(c *Copier) {
		c.logger = logger
	}
}

// WithReportingFunction sets the function that will be called at
// reportingPeriod with information about the copy progress
func WithReportingFunction(f ReportFunc) Option {
	return func(c *Copier) {
		c.reportingFunction = f
	}
}
