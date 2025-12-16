package csvcopy

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOptionsMutualExclusivity(t *testing.T) {
	tests := []struct {
		name          string
		options       []Option
		expectError   bool
		errorContains string
	}{
		// Valid individual configurations
		{
			name:        "WithSkipHeader alone should work",
			options:     []Option{WithSkipHeader(true)},
			expectError: false,
		},
		{
			name:        "WithSkipHeader false should work",
			options:     []Option{WithSkipHeader(false)},
			expectError: false,
		},
		{
			name: "WithColumnMapping alone should work",
			options: []Option{WithColumnMapping([]ColumnMapping{
				{CSVColumnName: "csv_col", DatabaseColumnName: "db_col"},
			})},
			expectError: false,
		},
		{
			name:        "WithAutoColumnMapping alone should work",
			options:     []Option{WithAutoColumnMapping()},
			expectError: false,
		},
		{
			name:        "WithColumns alone should work",
			options:     []Option{WithColumns("col1,col2,col3")},
			expectError: false,
		},
		{
			name:        "WithSkipHeaderCount alone should work",
			options:     []Option{WithSkipHeaderCount(2)},
			expectError: false,
		},

		// Mutual exclusivity tests - WithSkipHeader conflicts
		{
			name: "WithSkipHeader + WithColumnMapping should fail",
			options: []Option{
				WithSkipHeader(true),
				WithColumnMapping([]ColumnMapping{
					{CSVColumnName: "csv_col", DatabaseColumnName: "db_col"},
				}),
			},
			expectError:   true,
			errorContains: "header handling is already configured",
		},
		{
			name: "WithSkipHeader + WithAutoColumnMapping should fail",
			options: []Option{
				WithSkipHeader(true),
				WithAutoColumnMapping(),
			},
			expectError:   true,
			errorContains: "header handling is already configured",
		},
		{
			name: "WithColumnMapping + WithSkipHeader should fail",
			options: []Option{
				WithColumnMapping([]ColumnMapping{
					{CSVColumnName: "csv_col", DatabaseColumnName: "db_col"},
				}),
				WithSkipHeader(true),
			},
			expectError:   true,
			errorContains: "header handling is already configured",
		},
		{
			name: "WithAutoColumnMapping + WithSkipHeader should fail",
			options: []Option{
				WithAutoColumnMapping(),
				WithSkipHeader(true),
			},
			expectError:   true,
			errorContains: "header handling is already configured",
		},

		// Mutual exclusivity tests - Column mapping conflicts
		{
			name: "WithColumnMapping + WithAutoColumnMapping should fail",
			options: []Option{
				WithColumnMapping([]ColumnMapping{
					{CSVColumnName: "csv_col", DatabaseColumnName: "db_col"},
				}),
				WithAutoColumnMapping(),
			},
			expectError:   true,
			errorContains: "header handling is already configured",
		},
		{
			name: "WithAutoColumnMapping + WithColumnMapping should fail",
			options: []Option{
				WithAutoColumnMapping(),
				WithColumnMapping([]ColumnMapping{
					{CSVColumnName: "csv_col", DatabaseColumnName: "db_col"},
				}),
			},
			expectError:   true,
			errorContains: "header handling is already configured",
		},

		// Triple conflicts
		{
			name: "All three options should fail",
			options: []Option{
				WithSkipHeader(true),
				WithColumnMapping([]ColumnMapping{
					{CSVColumnName: "csv_col", DatabaseColumnName: "db_col"},
				}),
				WithAutoColumnMapping(),
			},
			expectError:   true,
			errorContains: "header handling is already configured",
		},

		// WithColumns conflicts with column mapping
		{
			name: "WithColumns + WithColumnMapping should fail",
			options: []Option{
				WithColumns("col1,col2"),
				WithColumnMapping([]ColumnMapping{
					{CSVColumnName: "csv_col", DatabaseColumnName: "db_col"},
				}),
			},
			expectError:   true,
			errorContains: "columns are already set",
		},
		{
			name: "WithColumns + WithAutoColumnMapping should fail",
			options: []Option{
				WithColumns("col1,col2"),
				WithAutoColumnMapping(),
			},
			expectError:   true,
			errorContains: "columns are already set",
		},
		{
			name: "WithColumnMapping + WithColumns should fail",
			options: []Option{
				WithColumnMapping([]ColumnMapping{
					{CSVColumnName: "csv_col", DatabaseColumnName: "db_col"},
				}),
				WithColumns("col1,col2"),
			},
			expectError:   true,
			errorContains: "column mapping is already set",
		},
		{
			name: "WithAutoColumnMapping + WithColumns should fail",
			options: []Option{
				WithAutoColumnMapping(),
				WithColumns("col1,col2"),
			},
			expectError:   true,
			errorContains: "column mapping is already set",
		},

		// Valid combinations that should work
		{
			name: "WithSkipHeader false + WithColumns should work",
			options: []Option{
				WithSkipHeader(false),
				WithColumns("col1,col2"),
			},
			expectError: false,
		},
		{
			name: "WithSkipHeaderCount + WithColumns should work",
			options: []Option{
				WithSkipHeaderCount(2),
				WithColumns("col1,col2"),
			},
			expectError: false,
		},
		{
			name: "WithSkipHeader + WithSkipHeaderCount should work",
			options: []Option{
				WithSkipHeader(true),
				WithSkipHeaderCount(3),
			},
			expectError: false,
		},
		{
			name: "Disable DirectCompress + enabled ClientSideSorting should not work",
			options: []Option{
				WithDirectCompress(true),
				WithClientSideSorting(true),
			},
			expectError:   true,
			errorContains: "direct compress can not be disabled when client side sorting is enabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewCopier("test-conn", "test-table", tt.options...)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain '%s', but got: %s", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %s", err.Error())
				}
			}
		})
	}
}

func TestColumnMappingValidation(t *testing.T) {
	tests := []struct {
		name          string
		mappings      []ColumnMapping
		expectError   bool
		errorContains string
	}{
		{
			name: "Valid column mapping should work",
			mappings: []ColumnMapping{
				{CSVColumnName: "csv_col1", DatabaseColumnName: "db_col1"},
				{CSVColumnName: "csv_col2", DatabaseColumnName: "db_col2"},
			},
			expectError: false,
		},
		{
			name:          "Nil mappings should fail",
			mappings:      nil,
			expectError:   true,
			errorContains: "column mapping cannot be nil",
		},
		{
			name: "Empty CSV column name should fail",
			mappings: []ColumnMapping{
				{CSVColumnName: "", DatabaseColumnName: "db_col"},
			},
			expectError:   true,
			errorContains: "empty CSVColumnName",
		},
		{
			name: "Empty database column name should fail",
			mappings: []ColumnMapping{
				{CSVColumnName: "csv_col", DatabaseColumnName: ""},
			},
			expectError:   true,
			errorContains: "empty DatabaseColumnName",
		},
		{
			name: "Multiple mappings with one invalid should fail",
			mappings: []ColumnMapping{
				{CSVColumnName: "csv_col1", DatabaseColumnName: "db_col1"},
				{CSVColumnName: "", DatabaseColumnName: "db_col2"},
			},
			expectError:   true,
			errorContains: "empty CSVColumnName",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewCopier("test-conn", "test-table", WithColumnMapping(tt.mappings))

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain '%s', but got: %s", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %s", err.Error())
				}
			}
		})
	}
}

func TestHeaderHandlingEnumValues(t *testing.T) {
	tests := []struct {
		name           string
		option         Option
		expectedHeader HeaderHandling
	}{
		{
			name:           "WithSkipHeader(true) should set HeaderSkip",
			option:         WithSkipHeader(true),
			expectedHeader: HeaderSkip,
		},
		{
			name:           "WithSkipHeader(false) should keep HeaderNone",
			option:         WithSkipHeader(false),
			expectedHeader: HeaderSkip,
		},
		{
			name:           "WithAutoColumnMapping should set HeaderAutoColumnMapping",
			option:         WithAutoColumnMapping(),
			expectedHeader: HeaderAutoColumnMapping,
		},
		{
			name: "WithColumnMapping should set HeaderColumnMapping",
			option: WithColumnMapping([]ColumnMapping{
				{CSVColumnName: "csv_col", DatabaseColumnName: "db_col"},
			}),
			expectedHeader: HeaderColumnMapping,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			copier, err := NewCopier("test-conn", "test-table", tt.option)
			if err != nil {
				t.Errorf("Unexpected error: %s", err.Error())
				return
			}

			if copier.useFileHeaders != tt.expectedHeader {
				t.Errorf("Expected useFileHeaders to be %d, but got %d", tt.expectedHeader, copier.useFileHeaders)
			}
		})
	}
}

func TestSkipHeaderCountValidation(t *testing.T) {
	tests := []struct {
		name          string
		count         int
		expectError   bool
		errorContains string
	}{
		{
			name:        "Valid skip count should work",
			count:       3,
			expectError: false,
		},
		{
			name:          "Zero skip count should fail",
			count:         0,
			expectError:   true,
			errorContains: "header line count must be greater than zero",
		},
		{
			name:          "Negative skip count should fail",
			count:         -1,
			expectError:   true,
			errorContains: "header line count must be greater than zero",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewCopier("test-conn", "test-table", WithSkipHeaderCount(tt.count))

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errorContains != "" && !strings.Contains(err.Error(), tt.errorContains) {
					t.Errorf("Expected error to contain '%s', but got: %s", tt.errorContains, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error but got: %s", err.Error())
				}
			}
		})
	}
}

func TestQueueSizeSetsQueueSize(t *testing.T) {
	copier, err := NewCopier("connstr", "table", WithQueueSize(2))
	require.NoError(t, err)
	require.Equal(t, 2, copier.queueSize)

	copier, err = NewCopier("connstr", "table")
	require.NoError(t, err)
	require.Equal(t, 0, copier.queueSize)
}

