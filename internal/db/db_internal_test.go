package db

import (
	"os"
	"testing"
)

type badOverride string

func (o badOverride) Override() string {
	return "fail"
}

func TestParseConnStr(t *testing.T) {
	cases := []struct {
		desc      string
		input     string
		overrides []Overrideable
		envs      map[string]string
		want      *minimalConnConfig
		wantErr   string
	}{
		{
			desc:  "simple DSN",
			input: "host=localhost user=test",
			want: &minimalConnConfig{
				host: "localhost",
				user: "test",
				port: 5432,
			},
		},
		{
			desc:  "simple URL",
			input: "postgres://test@localhost",
			want: &minimalConnConfig{
				host: "localhost",
				user: "test",
				port: 5432,
			},
		},
		{
			desc:  "full URL",
			input: "postgres://test:secretPassword!!+!@example:7182/foo?sslmode=require",
			want: &minimalConnConfig{
				host:     "example",
				user:     "test",
				password: "secretPassword!!+!",
				port:     7182,
				db:       "foo",
				sslmode:  "require",
			},
		},
		{
			desc:      "DSN w/ override",
			input:     "host=localhost user=test dbname=foo",
			overrides: []Overrideable{OverrideDBName("bar")},
			want: &minimalConnConfig{
				host: "localhost",
				user: "test",
				port: 5432,
				db:   "bar",
			},
		},
		{
			desc:      "unknown override",
			input:     "host=localhost",
			overrides: []Overrideable{badOverride("fail")},
			wantErr:   "unknown overrideable: db.badOverride=fail",
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			if c.envs != nil {
				for k, v := range c.envs {
					if err := os.Setenv(k, v); err != nil {
						t.Errorf("could not set env %s=%s", k, v)
						return
					}
				}
			}

			mcc, err := parseConnStr(c.input, c.overrides...)
			if c.wantErr == "" {
				if err != nil {
					t.Errorf("unexpected error parsing: %v", err)
				}
				if got := mcc.DSN(); got != c.want.DSN() {
					t.Errorf("incorrect result:\ngot\n%s\nwant\n%s", got, c.want.DSN())
				}
			} else {
				if err == nil {
					t.Errorf("unexpected lack of error")
				}
				if got := err.Error(); got != c.wantErr {
					t.Errorf("incorrect error:\ngot\n%s\nwant\n%s", got, c.wantErr)
				}
			}
		})
	}
}

func TestDetermineTLS(t *testing.T) {
	cases := []struct {
		desc    string
		input   string
		envs    map[string]string
		want    string
		wantErr error
	}{
		{
			desc:  "DSN valid (require)",
			input: "host=localhost sslmode=require",
			want:  "require",
		},
		{
			desc:  "URL valid (disable)",
			input: "postgres://localhost?sslmode=disable",
			want:  "disable",
		},
		{
			desc:  "URL valid (allow)",
			input: "postgresql://localhost?sslmode=allow",
			want:  "allow",
		},
		{
			desc:  "DSN valid (prefer)",
			input: "host=localhost sslmode=prefer",
			want:  "prefer",
		},
		{
			desc:  "DSN valid (verify-ca)",
			input: "host=localhost sslmode=verify-ca port=1234",
			want:  "verify-ca",
		},
		{
			desc:  "DSN valid (verify-full)",
			input: "sslmode=verify-full",
			want:  "verify-full",
		},
		{
			desc:    "DSN invalid",
			input:   "sslmode=preferred",
			wantErr: &ErrInvalidSSLMode{given: "preferred"},
		},
		{
			desc:  "missing, no env",
			input: "host=localhost",
			want:  "",
		},
		{
			desc:  "missing, valid env",
			input: "host=localhost",
			envs:  map[string]string{envSSLMode: "prefer"},
			want:  "prefer",
		},
		{
			desc:    "missing, invalid env",
			envs:    map[string]string{envSSLMode: "who"},
			wantErr: &ErrInvalidSSLMode{given: "who"},
		},
		{
			desc: "missing, no env",
			want: "",
		},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			os.Unsetenv(envSSLMode)
			if c.envs != nil {
				for k, v := range c.envs {
					if err := os.Setenv(k, v); err != nil {
						t.Errorf("could not set env %s with value %s", k, v)
						return
					}
				}
			}
			got, err := determineTLS(c.input)
			if c.wantErr == nil {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				} else if got != c.want {
					t.Errorf("incorrect ssl mode: got %s want %s", got, c.want)
				}
			} else {
				if err == nil {
					t.Errorf("unexpected lack of error")
				} else if err.Error() != c.wantErr.Error() {
					t.Errorf("incorrect error:\ngot\n%v\nwant\n%v", got, c.wantErr)
				}
			}
		})
	}
}
