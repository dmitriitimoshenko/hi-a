package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
)

type SheetsClientMock struct {
	mock.Mock
}

func (m *SheetsClientMock) Read(ctx context.Context, sheetRange string, spreadSheetID, spreadSheetPage *string) ([][]string, error) {
	args := m.Called(ctx, sheetRange, spreadSheetID, spreadSheetPage)

	result, _ := args.Get(0).([][]string)

	return result, args.Error(1)
}

func (m *SheetsClientMock) Write(ctx context.Context, value string, ceil string) error {
	args := m.Called(ctx, value, ceil)

	return args.Error(0)
}
