package services_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/mocks"
	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/mock"
)

func newSheetsServiceWithMock() (*services.SheetsService, *mocks.SheetsClientMock) {
	client := &mocks.SheetsClientMock{}
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))

	return services.NewSheetsService(logger, client), client
}

func TestGetApplicationFromRow(t *testing.T) {
	t.Parallel()

	successRow := []string{
		"Acme", "full-time", "remote", "Dev", "1-2", "3-4", "USD", "monthly", "applied", "50000",
	}

	tests := []struct {
		name     string
		rowIndex int
		rows     [][]string
		readErr  error
		assert   func(t *testing.T, app *dto.SheetApplicationDTO, err error)
	}{
		{
			name:     "success",
			rowIndex: 3,
			rows:     [][]string{successRow},
			assert: func(t *testing.T, app *dto.SheetApplicationDTO, err error) {
				assert.Equal(t, err, nil)
				if app == nil {
					t.Fatalf("expected application")
				}
				assert.Equal(t, app.Company, "Acme")
			},
		},
		{
			name:     "read error",
			rowIndex: 1,
			readErr:  errors.New("boom"),
			assert: func(t *testing.T, _ *dto.SheetApplicationDTO, err error) {
				assert.Equal(t, err == nil, false)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			service, client := newSheetsServiceWithMock()
			rangeStr := fmt.Sprintf("A%d:P%d", tt.rowIndex, tt.rowIndex)
			client.On("Read", mock.Anything, rangeStr, (*string)(nil), (*string)(nil)).
				Return(tt.rows, tt.readErr).Once()

			app, err := service.GetApplicationFromRow(context.Background(), int64(tt.rowIndex))

			tt.assert(t, app, err)
			client.AssertExpectations(t)
		})
	}
}

func TestGetApplicationsFromRows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		startRow  int
		endRow    int
		rows      [][]string
		assertion func(t *testing.T, result map[int64]dto.SheetApplicationDTO, err error)
	}{
		{
			name:     "success",
			startRow: 3,
			endRow:   3,
			rows: [][]string{
				{
					"Acme", "full-time", "remote", "Dev", "1-2", "3-4", "USD", "monthly", "applied", "50000",
					"50001", "50002", "stage3", "contact", "jd", "notes",
				},
			},
			assertion: func(t *testing.T, result map[int64]dto.SheetApplicationDTO, err error) {
				assert.Equal(t, err, nil)
				assert.Equal(t, len(result), 1)

				app := result[3]
				assert.Equal(t, app.Company, "Acme")
				assert.Equal(t, app.EmploymentType, enums.EmploymentType("full-time"))
				assert.Equal(t, app.WorkMode, enums.WorkMode("remote"))
				assert.Equal(t, app.Title, "Dev")
				assert.Equal(t, app.Status, enums.ApplicationStatus("applied"))
				assert.Equal(t, app.Meta["contacts"], "contact")
				assert.Equal(t, app.Stage != nil, true)
				assert.Equal(t, *app.Stage, int64(3))
			},
		},
		{
			name:     "skips empty rows",
			startRow: 1,
			endRow:   2,
			rows: [][]string{
				{"Acme", "full-time", "remote", "Dev", "1-2", "3-4", "USD", "monthly", "applied", "50000"},
				{},
			},
			assertion: func(t *testing.T, result map[int64]dto.SheetApplicationDTO, err error) {
				assert.Equal(t, err, nil)
				assert.Equal(t, len(result), 1)
			},
		},
		{
			name:     "invalid mandatory fields",
			startRow: 1,
			endRow:   2,
			rows: [][]string{
				{"only", "two"},
				{"Acme", "full-time", "remote", "Dev", "1-2", "", "USD", "monthly", "applied", "50000"},
			},
			assertion: func(t *testing.T, _ map[int64]dto.SheetApplicationDTO, err error) {
				assert.Equal(t, err == nil, false)
			},
		},
		{
			name:     "missing mandatory fields causes error",
			startRow: 10,
			endRow:   11,
			rows: [][]string{
				{"", "full-time", "remote", "Dev", "1-2", "3-4", "USD", "monthly", "applied", "50000"},
				{"Acme", "full-time", "remote", "Dev", "1-2", "3-4", "USD", "monthly", "applied", "50000"},
			},
			assertion: func(t *testing.T, _ map[int64]dto.SheetApplicationDTO, err error) {
				assert.Equal(t, err == nil, false)
			},
		},
		{
			name:     "last row incomplete is skipped",
			startRow: 1,
			endRow:   2,
			rows: [][]string{
				{"Acme", "full-time", "remote", "Dev", "1-2", "", "USD", "monthly", "applied", "50000"},
				{},
			},
			assertion: func(t *testing.T, result map[int64]dto.SheetApplicationDTO, err error) {
				assert.Equal(t, err, nil)
				assert.Equal(t, len(result), 1)
			},
		},
		{
			name:     "salary parse error",
			startRow: 5,
			endRow:   5,
			rows: [][]string{
				{"Acme", "full-time", "remote", "Dev", "bad_salary", "", "USD", "monthly", "applied", "50000"},
			},
			assertion: func(t *testing.T, _ map[int64]dto.SheetApplicationDTO, err error) {
				assert.Equal(t, err == nil, false)
			},
		},
		{
			name:     "applied at parse error",
			startRow: 2,
			endRow:   2,
			rows: [][]string{
				{"Acme", "full-time", "remote", "Dev", "", "", "USD", "monthly", "applied", "not_a_date"},
			},
			assertion: func(t *testing.T, _ map[int64]dto.SheetApplicationDTO, err error) {
				assert.Equal(t, err == nil, false)
			},
		},
		{
			name:     "responded at parse error",
			startRow: 4,
			endRow:   4,
			rows: [][]string{
				{"Acme", "full-time", "remote", "Dev", "", "", "USD", "monthly", "applied", "50000", "bad_date"},
			},
			assertion: func(t *testing.T, _ map[int64]dto.SheetApplicationDTO, err error) {
				assert.Equal(t, err == nil, false)
			},
		},
		{
			name:     "next follow up parse error",
			startRow: 6,
			endRow:   6,
			rows: [][]string{
				{"Acme", "full-time", "remote", "Dev", "", "", "USD", "monthly", "applied", "50000", "50001", "bad"},
			},
			assertion: func(t *testing.T, _ map[int64]dto.SheetApplicationDTO, err error) {
				assert.Equal(t, err == nil, false)
			},
		},
		{
			name:     "stage parse error",
			startRow: 7,
			endRow:   7,
			rows: [][]string{
				{"Acme", "full-time", "remote", "Dev", "", "", "USD", "monthly", "applied", "50000", "50001", "50002", "not_a_number"},
			},
			assertion: func(t *testing.T, _ map[int64]dto.SheetApplicationDTO, err error) {
				assert.Equal(t, err == nil, false)
			},
		},
		{
			name:     "salary proposed parse error",
			startRow: 8,
			endRow:   8,
			rows: [][]string{
				{"Acme", "full-time", "remote", "Dev", "", "bad", "USD", "monthly", "applied", "50000"},
			},
			assertion: func(t *testing.T, _ map[int64]dto.SheetApplicationDTO, err error) {
				assert.Equal(t, err == nil, false)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			service, client := newSheetsServiceWithMock()
			client.On("Read", mock.Anything, fmt.Sprintf("A%d:P%d", tt.startRow, tt.endRow), (*string)(nil), (*string)(nil)).
				Return(tt.rows, nil).Once()

			result, err := service.GetApplicationsFromRows(context.Background(), int64(tt.startRow), int64(tt.endRow))

			tt.assertion(t, result, err)
			client.AssertExpectations(t)
		})
	}
}

func TestGetEmploymentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		row       int
		value     string
		expectErr bool
	}{
		{name: "invalid", row: 4, value: "invalid", expectErr: true},
		{name: "success", row: 2, value: "full-time"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			service, client := newSheetsServiceWithMock()

			client.On("Read", mock.Anything, fmt.Sprintf("B%d", tt.row), (*string)(nil), (*string)(nil)).
				Return([][]string{{tt.value}}, nil).Once()

			etype, err := service.GetEmploymentType(context.Background(), int64(tt.row))

			if tt.expectErr {
				assert.Equal(t, err == nil, false)
				client.AssertExpectations(t)
				return
			}

			assert.Equal(t, err, nil)
			assert.Equal(t, *etype, enums.EmploymentTypeFullTime)
			client.AssertExpectations(t)
		})
	}
}

func TestSetEmploymentTypeSuccess(t *testing.T) {
	service, client := newSheetsServiceWithMock()

	client.On("Write", mock.Anything, "full-time", "B3").Return(nil).Once()

	err := service.SetEmploymentType(context.Background(), 3, enums.EmploymentTypeFullTime)

	assert.Equal(t, err, nil)
	client.AssertExpectations(t)
}

func TestGetStatusInvalid(t *testing.T) {
	t.Parallel()

	service, client := newSheetsServiceWithMock()

	client.On("Read", mock.Anything, "I7", (*string)(nil), (*string)(nil)).
		Return([][]string{{"unknown"}}, nil).Once()

	_, err := service.GetStatus(context.Background(), 7)

	assert.Equal(t, err == nil, false)
	client.AssertExpectations(t)
}

func TestGetSalaryAppliedScenarios(t *testing.T) {
	t.Parallel()

	service, client := newSheetsServiceWithMock()

	// no data -> nil
	client.On("Read", mock.Anything, "E1", (*string)(nil), (*string)(nil)).Return([][]string{}, nil).Once()
	client.On("Read", mock.Anything, "G1", (*string)(nil), (*string)(nil)).Return([][]string{}, nil).Once()
	client.On("Read", mock.Anything, "H1", (*string)(nil), (*string)(nil)).Return([][]string{}, nil).Once()

	val, err := service.GetSalaryApplied(context.Background(), 1)
	assert.Equal(t, err, nil)
	assert.Equal(t, val, (*dto.SalaryDataDTO)(nil))

	// parse error
	client.On("Read", mock.Anything, "E2", (*string)(nil), (*string)(nil)).Return([][]string{{"bad"}}, nil).Once()
	client.On("Read", mock.Anything, "G2", (*string)(nil), (*string)(nil)).Return([][]string{{"USD"}}, nil).Once()
	client.On("Read", mock.Anything, "H2", (*string)(nil), (*string)(nil)).Return([][]string{{"monthly"}}, nil).Once()

	_, err = service.GetSalaryApplied(context.Background(), 2)
	assert.Equal(t, err == nil, false)

	// success with amount
	client.On("Read", mock.Anything, "E3", (*string)(nil), (*string)(nil)).Return([][]string{{"1-2"}}, nil).Once()
	client.On("Read", mock.Anything, "G3", (*string)(nil), (*string)(nil)).Return([][]string{{"USD"}}, nil).Once()
	client.On("Read", mock.Anything, "H3", (*string)(nil), (*string)(nil)).Return([][]string{{"monthly"}}, nil).Once()

	val, err = service.GetSalaryApplied(context.Background(), 3)
	assert.Equal(t, err, nil)
	if val == nil {
		t.Fatalf("expected salary data")
	}
	assert.Equal(t, *val.AmountFrom, float64(1))
	assert.Equal(t, val.Currency, "USD")

	// success without amount
	client.On("Read", mock.Anything, "E4", (*string)(nil), (*string)(nil)).Return([][]string{{""}}, nil).Once()
	client.On("Read", mock.Anything, "G4", (*string)(nil), (*string)(nil)).Return([][]string{{"EUR"}}, nil).Once()
	client.On("Read", mock.Anything, "H4", (*string)(nil), (*string)(nil)).Return([][]string{{"yearly"}}, nil).Once()

	val, err = service.GetSalaryApplied(context.Background(), 4)
	assert.Equal(t, err, nil)
	if val == nil {
		t.Fatalf("expected salary data")
	}
	assert.Equal(t, val.AmountFrom, (*float64)(nil))
	assert.Equal(t, val.Currency, "EUR")

	client.AssertExpectations(t)
}

func TestGetSalaryAppliedReadError(t *testing.T) {
	t.Parallel()

	service, client := newSheetsServiceWithMock()

	client.On("Read", mock.Anything, "E15", (*string)(nil), (*string)(nil)).Return(nil, errors.New("fail")).Once()

	_, err := service.GetSalaryApplied(context.Background(), 15)

	assert.Equal(t, err == nil, false)
	client.AssertExpectations(t)
}

func TestSetSalaryApplied(t *testing.T) {
	t.Parallel()

	amountFrom := 1.0
	amountTo := 2.0

	tests := []struct {
		name        string
		row         int
		salary      *dto.SalaryDataDTO
		setup       func(row int, client *mocks.SheetsClientMock)
		expectErr   bool
		expectPanic bool
	}{
		{
			name:   "nil no existing",
			row:    5,
			salary: nil,
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Read", mock.Anything, fmt.Sprintf("E%d", row), (*string)(nil), (*string)(nil)).Return([][]string{}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("G%d", row), (*string)(nil), (*string)(nil)).Return([][]string{}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("H%d", row), (*string)(nil), (*string)(nil)).Return([][]string{}, nil).Once()
				client.On("Write", mock.Anything, "", fmt.Sprintf("E%d:H%d", row, row)).Return(nil).Once()
			},
		},
		{
			name:   "nil existing panics",
			row:    6,
			salary: nil,
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Read", mock.Anything, fmt.Sprintf("E%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"1-2"}}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("G%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"USD"}}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("H%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"monthly"}}, nil).Once()
				client.On("Write", mock.Anything, "", fmt.Sprintf("E%d", row)).Return(nil).Once()
			},
			expectPanic: true,
		},
		{
			name: "with values",
			row:  7,
			salary: &dto.SalaryDataDTO{
				AmountFrom: &amountFrom,
				AmountTo:   &amountTo,
				Currency:   "USD",
				Period:     enums.SalaryPeriodMonthly,
			},
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Write", mock.Anything, "1-2", fmt.Sprintf("E%d", row)).Return(nil).Once()
				client.On("Write", mock.Anything, "USD", fmt.Sprintf("G%d", row)).Return(nil).Once()
				client.On("Write", mock.Anything, "monthly", fmt.Sprintf("H%d", row)).Return(nil).Once()
			},
		},
		{
			name: "write error",
			row:  12,
			salary: &dto.SalaryDataDTO{
				AmountFrom: &amountFrom,
				AmountTo:   &amountTo,
				Currency:   "USD",
				Period:     enums.SalaryPeriodMonthly,
			},
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Write", mock.Anything, "1-2", fmt.Sprintf("E%d", row)).Return(errors.New("fail")).Once()
			},
			expectErr: true,
		},
		{
			name: "with no amounts",
			row:  11,
			salary: &dto.SalaryDataDTO{
				Currency: "GBP",
				Period:   enums.SalaryPeriodMonthly,
			},
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Write", mock.Anything, "GBP", fmt.Sprintf("G%d", row)).Return(nil).Once()
				client.On("Write", mock.Anything, "monthly", fmt.Sprintf("H%d", row)).Return(nil).Once()
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			service, client := newSheetsServiceWithMock()
			tt.setup(tt.row, client)

			var (
				err           error
				panicOccurred bool
			)

			func() {
				defer func() {
					if r := recover(); r != nil {
						panicOccurred = true
					}
				}()

				err = service.SetSalaryApplied(context.Background(), int64(tt.row), tt.salary)
			}()

			if tt.expectPanic {
				if !panicOccurred {
					t.Fatalf("expected panic")
				}
				client.AssertExpectations(t)
				return
			}

			if panicOccurred {
				t.Fatalf("did not expect panic")
			}

			if tt.expectErr {
				assert.Equal(t, err == nil, false)
			} else {
				assert.Equal(t, err, nil)
			}

			client.AssertExpectations(t)
		})
	}
}

func TestGetSalaryProposed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		row    int
		setup  func(row int, client *mocks.SheetsClientMock)
		assert func(t *testing.T, val *dto.SalaryDataDTO, err error)
	}{
		{
			name: "success",
			row:  3,
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Read", mock.Anything, fmt.Sprintf("F%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"5-6"}}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("G%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"USD"}}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("H%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"monthly"}}, nil).Once()
			},
			assert: func(t *testing.T, val *dto.SalaryDataDTO, err error) {
				assert.Equal(t, err, nil)
				if val == nil {
					t.Fatalf("expected salary data")
				}
				assert.Equal(t, *val.AmountFrom, float64(5))
				assert.Equal(t, val.Currency, "USD")
			},
		},
		{
			name: "no amount",
			row:  13,
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Read", mock.Anything, fmt.Sprintf("F%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{""}}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("G%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"USD"}}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("H%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"monthly"}}, nil).Once()
			},
			assert: func(t *testing.T, val *dto.SalaryDataDTO, err error) {
				assert.Equal(t, err, nil)
				if val == nil {
					t.Fatalf("expected salary data")
				}
				assert.Equal(t, val.AmountFrom, (*float64)(nil))
			},
		},
		{
			name: "nil",
			row:  11,
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Read", mock.Anything, fmt.Sprintf("F%d", row), (*string)(nil), (*string)(nil)).Return([][]string{}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("G%d", row), (*string)(nil), (*string)(nil)).Return([][]string{}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("H%d", row), (*string)(nil), (*string)(nil)).Return([][]string{}, nil).Once()
			},
			assert: func(t *testing.T, val *dto.SalaryDataDTO, err error) {
				assert.Equal(t, err, nil)
				assert.Equal(t, val, (*dto.SalaryDataDTO)(nil))
			},
		},
		{
			name: "read error",
			row:  17,
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Read", mock.Anything, fmt.Sprintf("F%d", row), (*string)(nil), (*string)(nil)).Return(nil, errors.New("fail")).Once()
			},
			assert: func(t *testing.T, _ *dto.SalaryDataDTO, err error) {
				assert.Equal(t, err == nil, false)
			},
		},
		{
			name: "parse error",
			row:  16,
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Read", mock.Anything, fmt.Sprintf("F%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"bad"}}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("G%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"USD"}}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("H%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"monthly"}}, nil).Once()
			},
			assert: func(t *testing.T, _ *dto.SalaryDataDTO, err error) {
				assert.Equal(t, err == nil, false)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			service, client := newSheetsServiceWithMock()
			tt.setup(tt.row, client)

			val, err := service.GetSalaryProposed(context.Background(), int64(tt.row))

			tt.assert(t, val, err)
			client.AssertExpectations(t)
		})
	}
}

func TestSetSalaryProposed(t *testing.T) {
	t.Parallel()

	amountFrom := 10.0
	amountTo := 20.0
	amountFromErr := 1.0
	amountToErr := 2.0

	tests := []struct {
		name        string
		row         int
		salary      *dto.SalaryDataDTO
		setup       func(row int, client *mocks.SheetsClientMock)
		expectErr   bool
		expectPanic bool
	}{
		{
			name:   "nil no existing",
			row:    8,
			salary: nil,
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Read", mock.Anything, fmt.Sprintf("F%d", row), (*string)(nil), (*string)(nil)).Return([][]string{}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("G%d", row), (*string)(nil), (*string)(nil)).Return([][]string{}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("H%d", row), (*string)(nil), (*string)(nil)).Return([][]string{}, nil).Once()
				client.On("Write", mock.Anything, "", fmt.Sprintf("E%d:H%d", row, row)).Return(nil).Once()
			},
		},
		{
			name:   "nil existing panics",
			row:    9,
			salary: nil,
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Read", mock.Anything, fmt.Sprintf("F%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"5-6"}}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("G%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"USD"}}, nil).Once()
				client.On("Read", mock.Anything, fmt.Sprintf("H%d", row), (*string)(nil), (*string)(nil)).Return([][]string{{"monthly"}}, nil).Once()
				client.On("Write", mock.Anything, "", fmt.Sprintf("F%d", row)).Return(nil).Once()
			},
			expectPanic: true,
		},
		{
			name: "with values",
			row:  10,
			salary: &dto.SalaryDataDTO{
				AmountFrom: &amountFrom,
				AmountTo:   &amountTo,
				Currency:   "EUR",
				Period:     enums.SalaryPeriodYearly,
			},
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Write", mock.Anything, "10-20", fmt.Sprintf("F%d", row)).Return(nil).Once()
				client.On("Write", mock.Anything, "EUR", fmt.Sprintf("G%d", row)).Return(nil).Once()
				client.On("Write", mock.Anything, "yearly", fmt.Sprintf("H%d", row)).Return(nil).Once()
			},
		},
		{
			name: "with no amounts",
			row:  12,
			salary: &dto.SalaryDataDTO{
				Currency: "CHF",
				Period:   enums.SalaryPeriodMonthly,
			},
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Write", mock.Anything, "CHF", fmt.Sprintf("G%d", row)).Return(nil).Once()
				client.On("Write", mock.Anything, "monthly", fmt.Sprintf("H%d", row)).Return(nil).Once()
			},
		},
		{
			name: "write error",
			row:  14,
			salary: &dto.SalaryDataDTO{
				AmountFrom: &amountFromErr,
				AmountTo:   &amountToErr,
				Currency:   "USD",
				Period:     enums.SalaryPeriodMonthly,
			},
			setup: func(row int, client *mocks.SheetsClientMock) {
				client.On("Write", mock.Anything, "1-2", fmt.Sprintf("F%d", row)).Return(errors.New("fail")).Once()
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			service, client := newSheetsServiceWithMock()
			tt.setup(tt.row, client)

			var (
				err           error
				panicOccurred bool
			)

			func() {
				defer func() {
					if r := recover(); r != nil {
						panicOccurred = true
					}
				}()

				err = service.SetSalaryProposed(context.Background(), int64(tt.row), tt.salary)
			}()

			if tt.expectPanic {
				if !panicOccurred {
					t.Fatalf("expected panic")
				}
				client.AssertExpectations(t)
				return
			}

			if panicOccurred {
				t.Fatalf("did not expect panic")
			}

			if tt.expectErr {
				assert.Equal(t, err == nil, false)
			} else {
				assert.Equal(t, err, nil)
			}

			client.AssertExpectations(t)
		})
	}
}

func TestGetSetPrimitiveFields(t *testing.T) {
	service, client := newSheetsServiceWithMock()

	client.On("Read", mock.Anything, "A1", (*string)(nil), (*string)(nil)).Return([][]string{{"Acme"}}, nil).Once()
	client.On("Write", mock.Anything, "Acme", "A1").Return(nil).Once()

	client.On("Read", mock.Anything, "D2", (*string)(nil), (*string)(nil)).Return([][]string{{"Dev"}}, nil).Once()
	client.On("Write", mock.Anything, "Dev", "D2").Return(nil).Once()

	client.On("Read", mock.Anything, "M3", (*string)(nil), (*string)(nil)).Return([][]string{{"3"}}, nil).Once()
	client.On("Write", mock.Anything, "", "M3").Return(nil).Once()

	client.On("Read", mock.Anything, "N4", (*string)(nil), (*string)(nil)).Return([][]string{{"contact"}}, nil).Once()
	client.On("Write", mock.Anything, "contact", "N4").Return(nil).Once()

	client.On("Read", mock.Anything, "O5", (*string)(nil), (*string)(nil)).Return([][]string{{"jd"}}, nil).Once()
	client.On("Write", mock.Anything, "jd", "O5").Return(nil).Once()

	client.On("Read", mock.Anything, "P6", (*string)(nil), (*string)(nil)).Return([][]string{{"notes"}}, nil).Once()
	client.On("Write", mock.Anything, "notes", "P6").Return(nil).Once()

	company, _ := service.GetCompany(context.Background(), 1)
	assert.Equal(t, *company, "Acme")
	assert.Equal(t, service.SetCompany(context.Background(), 1, "Acme"), nil)

	title, _ := service.GetTitle(context.Background(), 2)
	assert.Equal(t, *title, "Dev")
	assert.Equal(t, service.SetTitle(context.Background(), 2, "Dev"), nil)

	stage, _ := service.GetStage(context.Background(), 3)
	assert.Equal(t, *stage, "3")
	assert.Equal(t, service.SetStage(context.Background(), 3, nil), nil)

	contacts, _ := service.GetContacts(context.Background(), 4)
	assert.Equal(t, *contacts, "contact")
	assert.Equal(t, service.SetContacts(context.Background(), 4, "contact"), nil)

	jd, _ := service.GetJobDescription(context.Background(), 5)
	assert.Equal(t, *jd, "jd")
	assert.Equal(t, service.SetJobDescription(context.Background(), 5, "jd"), nil)

	notes, _ := service.GetNotes(context.Background(), 6)
	assert.Equal(t, *notes, "notes")
	assert.Equal(t, service.SetNotes(context.Background(), 6, "notes"), nil)

	client.AssertExpectations(t)
}

func TestGetSetDatesAndStatus(t *testing.T) {
	service, client := newSheetsServiceWithMock()

	now := time.Date(2024, time.May, 10, 15, 30, 0, 0, time.UTC)
	appliedStr := now.Format("02/01/2006")
	respondedStr := now.Format("02/01/2006")
	nextFollowUpStr := now.Format("02/01/2006 15:04:05")

	client.On("Read", mock.Anything, "I1", (*string)(nil), (*string)(nil)).Return([][]string{{"applied"}}, nil).Once()
	client.On("Write", mock.Anything, "applied", "I1").Return(nil).Once()

	client.On("Read", mock.Anything, "J1", (*string)(nil), (*string)(nil)).Return([][]string{{appliedStr}}, nil).Once()
	client.On("Write", mock.Anything, appliedStr, "J1").Return(nil).Once()

	client.On("Read", mock.Anything, "K1", (*string)(nil), (*string)(nil)).Return([][]string{{respondedStr}}, nil).Once()
	client.On("Write", mock.Anything, respondedStr, "K1").Return(nil).Once()

	client.On("Read", mock.Anything, "L1", (*string)(nil), (*string)(nil)).Return([][]string{{nextFollowUpStr}}, nil).Once()
	client.On("Write", mock.Anything, nextFollowUpStr, "L1").Return(nil).Once()

	status, err := service.GetStatus(context.Background(), 1)
	assert.Equal(t, err, nil)
	assert.Equal(t, *status, enums.ApplicationStatusApplied)
	assert.Equal(t, service.SetStatus(context.Background(), 1, enums.ApplicationStatusApplied), nil)

	appliedAt, err := service.GetAppliedAt(context.Background(), 1)
	assert.Equal(t, err, nil)
	assert.Equal(t, appliedAt.Format("02/01/2006"), appliedStr)
	assert.Equal(t, service.SetAppliedAt(context.Background(), 1, now), nil)

	respondedAt, err := service.GetRespondedAt(context.Background(), 1)
	assert.Equal(t, err, nil)
	assert.Equal(t, respondedAt.Format("02/01/2006"), respondedStr)
	assert.Equal(t, service.SetRespondedAt(context.Background(), 1, respondedAt), nil)

	nextFollowUpAt, err := service.GetNextFollowUpAt(context.Background(), 1)
	assert.Equal(t, err, nil)
	assert.Equal(t, nextFollowUpAt.Format(time.RFC3339), now.Format(time.RFC3339))
	assert.Equal(t, service.SetNextFollowUpAt(context.Background(), 1, nextFollowUpAt), nil)

	client.AssertExpectations(t)
}

func TestGetAppliedAtParseError(t *testing.T) {
	service, client := newSheetsServiceWithMock()

	client.On("Read", mock.Anything, "J2", (*string)(nil), (*string)(nil)).Return([][]string{{"bad"}}, nil).Once()

	_, err := service.GetAppliedAt(context.Background(), 2)

	assert.Equal(t, err == nil, false)
	client.AssertExpectations(t)
}

func TestGetRespondedAtParseError(t *testing.T) {
	service, client := newSheetsServiceWithMock()

	client.On("Read", mock.Anything, "K2", (*string)(nil), (*string)(nil)).Return([][]string{{"bad"}}, nil).Once()

	_, err := service.GetRespondedAt(context.Background(), 2)

	assert.Equal(t, err == nil, false)
	client.AssertExpectations(t)
}

func TestGetNextFollowUpAtParseError(t *testing.T) {
	service, client := newSheetsServiceWithMock()

	client.On("Read", mock.Anything, "L2", (*string)(nil), (*string)(nil)).Return([][]string{{"bad"}}, nil).Once()

	_, err := service.GetNextFollowUpAt(context.Background(), 2)

	assert.Equal(t, err == nil, false)
	client.AssertExpectations(t)
}

func TestGetWorkMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		row       int
		value     string
		expectErr bool
	}{
		{name: "success", row: 2, value: "remote"},
		{name: "invalid", row: 5, value: "invalid", expectErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			service, client := newSheetsServiceWithMock()

			client.On("Read", mock.Anything, fmt.Sprintf("C%d", tt.row), (*string)(nil), (*string)(nil)).
				Return([][]string{{tt.value}}, nil).Once()

			mode, err := service.GetWorkMode(context.Background(), int64(tt.row))

			if tt.expectErr {
				assert.Equal(t, err == nil, false)
				client.AssertExpectations(t)
				return
			}

			assert.Equal(t, err, nil)
			assert.Equal(t, *mode, enums.WorkModeRemote)
			client.AssertExpectations(t)
		})
	}
}

func TestSetWorkModeSuccess(t *testing.T) {
	service, client := newSheetsServiceWithMock()

	client.On("Write", mock.Anything, "hybrid", "C3").Return(nil).Once()

	err := service.SetWorkMode(context.Background(), 3, enums.WorkModeHybrid)

	assert.Equal(t, err, nil)
	client.AssertExpectations(t)
}

func TestSettersErrorPaths(t *testing.T) {
	service, client := newSheetsServiceWithMock()
	writeErr := errors.New("write failed")

	client.On("Write", mock.Anything, "Acme", "A1").Return(writeErr).Once()
	assert.Equal(t, service.SetCompany(context.Background(), 1, "Acme") == nil, false)

	client.On("Write", mock.Anything, "full-time", "B1").Return(writeErr).Once()
	assert.Equal(t, service.SetEmploymentType(context.Background(), 1, enums.EmploymentTypeFullTime) == nil, false)

	client.On("Write", mock.Anything, "remote", "C1").Return(writeErr).Once()
	assert.Equal(t, service.SetWorkMode(context.Background(), 1, enums.WorkModeRemote) == nil, false)

	client.On("Write", mock.Anything, "Dev", "D1").Return(writeErr).Once()
	assert.Equal(t, service.SetTitle(context.Background(), 1, "Dev") == nil, false)

	client.On("Write", mock.Anything, "applied", "I1").Return(writeErr).Once()
	assert.Equal(t, service.SetStatus(context.Background(), 1, enums.ApplicationStatusApplied) == nil, false)

	client.On("Write", mock.Anything, "01/01/2024", "J1").Return(writeErr).Once()
	assert.Equal(t, service.SetAppliedAt(context.Background(), 1, time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)) == nil, false)

	client.On("Write", mock.Anything, "01/01/2024", "K1").Return(writeErr).Once()
	ts := time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, service.SetRespondedAt(context.Background(), 1, &ts) == nil, false)

	client.On("Write", mock.Anything, "01/01/2024 00:00:00", "L1").Return(writeErr).Once()
	assert.Equal(t, service.SetNextFollowUpAt(context.Background(), 1, &ts) == nil, false)

	client.On("Write", mock.Anything, "1", "M1").Return(writeErr).Once()
	stage := int64(1)
	assert.Equal(t, service.SetStage(context.Background(), 1, &stage) == nil, false)

	client.On("Write", mock.Anything, "contacts", "N1").Return(writeErr).Once()
	assert.Equal(t, service.SetContacts(context.Background(), 1, "contacts") == nil, false)

	client.On("Write", mock.Anything, "jd", "O1").Return(writeErr).Once()
	assert.Equal(t, service.SetJobDescription(context.Background(), 1, "jd") == nil, false)

	client.On("Write", mock.Anything, "notes", "P1").Return(writeErr).Once()
	assert.Equal(t, service.SetNotes(context.Background(), 1, "notes") == nil, false)

	client.AssertExpectations(t)
}

func TestGettersErrorPaths(t *testing.T) {
	service, client := newSheetsServiceWithMock()
	readErr := errors.New("read failed")

	client.On("Read", mock.Anything, "A1", mock.Anything, mock.Anything).Return(nil, readErr).Once()
	_, err := service.GetCompany(context.Background(), 1)
	assert.Equal(t, err == nil, false)

	client.On("Read", mock.Anything, "B1", mock.Anything, mock.Anything).Return(nil, readErr).Once()
	_, err = service.GetEmploymentType(context.Background(), 1)
	assert.Equal(t, err == nil, false)

	client.On("Read", mock.Anything, "C1", mock.Anything, mock.Anything).Return(nil, readErr).Once()
	_, err = service.GetWorkMode(context.Background(), 1)
	assert.Equal(t, err == nil, false)

	client.On("Read", mock.Anything, "D1", mock.Anything, mock.Anything).Return(nil, readErr).Once()
	_, err = service.GetTitle(context.Background(), 1)
	assert.Equal(t, err == nil, false)

	client.On("Read", mock.Anything, "I1", mock.Anything, mock.Anything).Return(nil, readErr).Once()
	_, err = service.GetStatus(context.Background(), 1)
	assert.Equal(t, err == nil, false)

	client.On("Read", mock.Anything, "J1", mock.Anything, mock.Anything).Return(nil, readErr).Once()
	_, err = service.GetAppliedAt(context.Background(), 1)
	assert.Equal(t, err == nil, false)

	client.On("Read", mock.Anything, "K1", mock.Anything, mock.Anything).Return(nil, readErr).Once()
	_, err = service.GetRespondedAt(context.Background(), 1)
	assert.Equal(t, err == nil, false)

	client.On("Read", mock.Anything, "L1", mock.Anything, mock.Anything).Return(nil, readErr).Once()
	_, err = service.GetNextFollowUpAt(context.Background(), 1)
	assert.Equal(t, err == nil, false)

	client.On("Read", mock.Anything, "M1", mock.Anything, mock.Anything).Return(nil, readErr).Once()
	_, err = service.GetStage(context.Background(), 1)
	assert.Equal(t, err == nil, false)

	client.On("Read", mock.Anything, "N1", mock.Anything, mock.Anything).Return(nil, readErr).Once()
	_, err = service.GetContacts(context.Background(), 1)
	assert.Equal(t, err == nil, false)

	client.On("Read", mock.Anything, "O1", mock.Anything, mock.Anything).Return(nil, readErr).Once()
	_, err = service.GetJobDescription(context.Background(), 1)
	assert.Equal(t, err == nil, false)

	client.On("Read", mock.Anything, "P1", mock.Anything, mock.Anything).Return(nil, readErr).Once()
	_, err = service.GetNotes(context.Background(), 1)
	assert.Equal(t, err == nil, false)

	client.AssertExpectations(t)
}

func TestGetSuccess(t *testing.T) {
	service, client := newSheetsServiceWithMock()

	client.On("Read", mock.Anything, "A1:B2", mock.MatchedBy(func(s *string) bool { return s != nil && *s == "sheet" }), mock.MatchedBy(func(s *string) bool { return s != nil && *s == "page" })).
		Return([][]string{{"v"}}, nil).Once()

	resp, err := service.Get(context.Background(), "sheet", "page", "A1", "B2")

	assert.Equal(t, err, nil)
	assert.Equal(t, len(resp), 1)
	client.AssertExpectations(t)
}

func TestGetErrorFromClientIsWrapped(t *testing.T) {
	service, client := newSheetsServiceWithMock()

	client.On("Read", mock.Anything, "A1:B1", mock.Anything, mock.Anything).
		Return(nil, errors.New("boom")).Once()

	_, err := service.Get(context.Background(), "sheet", "page", "A1", "B1")

	assert.Equal(t, err == nil, false)
	client.AssertExpectations(t)
}
