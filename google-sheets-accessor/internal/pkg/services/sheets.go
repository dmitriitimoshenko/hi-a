package services

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
)

type SheetsService struct {
	client sheetsClient
}

func NewSheetsService(client sheetsClient) *SheetsService {
	return &SheetsService{
		client: client,
	}
}

func (s *SheetsService) GetApplicationFromRow(ctx context.Context, rowID int64) (*dto.SheetApplicationDTO, error) {
	applications, err := s.GetApplicationsFromRows(ctx, rowID, rowID)
	if err != nil {
		return nil, err
	}

	// takes last and single map element
	var application dto.SheetApplicationDTO
	for v := range maps.Values(applications) {
		application = v
	}

	return &application, nil
}

func (s *SheetsService) Get(ctx context.Context, sheetID, sheetPage, ceilFrom, ceilTo string) ([][]string, error) {
	sheetRange := ceilFrom + ":" + ceilTo

	result, err := s.client.Read(ctx, sheetRange, &sheetID, &sheetPage)
	if err != nil {
		return nil, fmt.Errorf("failed to read sheet [%s] page [%s] range [%s:%s]", sheetID, sheetPage, ceilFrom, ceilTo)
	}

	return result, nil
}

func (s *SheetsService) GetApplicationsFromRows(ctx context.Context, rowFrom, rowTo int64) (map[int64]dto.SheetApplicationDTO, error) {
	sheetRange := "A" + strconv.FormatInt(rowFrom, 10) + ":P" + strconv.FormatInt(rowTo, 10)
	resp, err := s.client.Read(ctx, sheetRange, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", sheetRange, err)
	}

	result := make(map[int64]dto.SheetApplicationDTO, rowTo-rowFrom+1)

	rowCnt := rowFrom
	for _, row := range resp {
		if len(row) == 0 {
			return nil, fmt.Errorf("no data found in Google sheet in range [%s]", sheetRange)
		}
		if len(row) < 10 {
			return nil, fmt.Errorf("incomplete data in Google sheet in range [%s]", sheetRange)
		}

		appliedAt, err := tools.ParseSheetDateGivenInDaysSince(row[9], "02/01/2006")
		if err != nil {
			return nil, fmt.Errorf("failed to parse appliedAt value [%s] in row [%d] with content [%v]: %w", row[9], rowCnt, row, err)
		}
		if appliedAt == nil {
			return nil, errors.New("appliedAt is nil")
		}

		subResult := dto.SheetApplicationDTO{
			Company:        row[0],
			EmploymentType: enums.EmploymentType(row[1]),
			WorkMode:       enums.WorkMode(row[2]),
			Title:          row[3],
			Status:         enums.ApplicationStatus(row[8]),
			AppliedAt:      *appliedAt,
		}

		if row[6] != "" && row[7] != "" {
			if row[4] != "" {
				subResult.SalaryApplied = &dto.SheetApplicationSalaryDTO{
					Currency: row[6],
					Period:   enums.SalaryPeriod(row[7]),
				}
				salaryAppliedAmountFrom, salaryAppliedAmountTo, ok := tools.ParseSalaryRange(row[4])
				if !ok {
					return nil, fmt.Errorf("failed to parse salaryApplied amount value [%s] in row [%d]", row[4], rowCnt)
				}
				subResult.SalaryApplied.AmountFrom = &salaryAppliedAmountFrom
				subResult.SalaryApplied.AmountTo = &salaryAppliedAmountTo
			}
			if row[5] != "" {
				subResult.SalaryProposed = &dto.SheetApplicationSalaryDTO{
					Currency: row[6],
					Period:   enums.SalaryPeriod(row[7]),
				}
				salaryProposedAmountFrom, salaryProposedAmountTo, ok := tools.ParseSalaryRange(row[5])
				if !ok {
					return nil, fmt.Errorf("failed to parse salaryProposed amount value [%s] in row [%d]", row[5], rowCnt)
				}
				subResult.SalaryProposed.AmountFrom = &salaryProposedAmountFrom
				subResult.SalaryProposed.AmountTo = &salaryProposedAmountTo
			}
		}

		if len(row) > 10 && row[10] != "" {
			respondedAt, err := tools.ParseSheetDateGivenInDaysSince(row[10], "02/01/2006")
			if err != nil {
				return nil, fmt.Errorf("failed to parse respondedAt value [%s] in row [%d] with content [%v]: %w", row[10], rowCnt, row, err)
			}
			subResult.RespondedAt = respondedAt
		}

		if len(row) > 11 && row[11] != "" {
			nextFollowUpAt, err := time.Parse("02/01/2006 15:04:05", row[11])
			if err != nil {
				return nil, fmt.Errorf("failed to parse nextFollowUpAt value [%s] in row [%d]: %w", row[11], rowCnt, err)
			}
			subResult.NextFollowUpAt = &nextFollowUpAt
		}

		if len(row) > 12 && row[12] != "" {
			stage, err := strconv.ParseInt(row[12], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse stage value [%s] in row [%d]: %w", row[12], rowCnt, err)
			}
			subResult.Stage = &stage
		}

		meta := make(map[string]string)
		if len(row) > 13 {
			meta["contacts"] = row[13]
		}
		if len(row) > 14 {
			meta["job_description"] = row[14]
		}
		if len(row) > 15 {
			meta["notes"] = row[15]
		}
		if len(meta) > 0 {
			subResult.Meta = meta
		}

		result[rowCnt] = subResult
		rowCnt++
	}

	return result, nil
}

func (s *SheetsService) GetCompany(ctx context.Context, rowID int64) (*string, error) {
	ceil := "A" + strconv.FormatInt(rowID, 10)
	resp, err := s.client.Read(ctx, ceil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceil, err)
	}

	company := resp[0][0]

	return &company, nil
}

func (s *SheetsService) SetCompany(ctx context.Context, rowID int64, company string) error {
	ceil := "A" + strconv.FormatInt(rowID, 10)
	if err := s.client.Write(ctx, company, ceil); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceil, err)
	}

	return nil
}

func (s *SheetsService) GetEmploymentType(ctx context.Context, rowID int64) (*enums.EmploymentType, error) {
	ceil := "B" + strconv.FormatInt(rowID, 10)
	resp, err := s.client.Read(ctx, ceil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceil, err)
	}

	employmentType := enums.EmploymentType(resp[0][0])
	if !employmentType.IsValid() {
		return nil, fmt.Errorf("invalid employment type value [%s] in row [%d]", resp[0][0], rowID)
	}

	return &employmentType, nil
}

func (s *SheetsService) SetEmploymentType(ctx context.Context, rowID int64, employmentType enums.EmploymentType) error {
	ceil := "B" + strconv.FormatInt(rowID, 10)
	if err := s.client.Write(ctx, string(employmentType), ceil); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceil, err)
	}

	return nil
}

func (s *SheetsService) GetWorkMode(ctx context.Context, rowID int64) (*enums.WorkMode, error) {
	ceil := "C" + strconv.FormatInt(rowID, 10)
	resp, err := s.client.Read(ctx, ceil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceil, err)
	}

	workMode := enums.WorkMode(resp[0][0])
	if !workMode.IsValid() {
		return nil, fmt.Errorf("invalid work mode value [%s] in row [%d]", resp[0][0], rowID)
	}

	return &workMode, nil
}

func (s *SheetsService) SetWorkMode(ctx context.Context, rowID int64, workMode enums.WorkMode) error {
	ceil := "C" + strconv.FormatInt(rowID, 10)
	if err := s.client.Write(ctx, string(workMode), ceil); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceil, err)
	}

	return nil
}

func (s *SheetsService) GetTitle(ctx context.Context, rowID int64) (*string, error) {
	ceil := "D" + strconv.FormatInt(rowID, 10)
	resp, err := s.client.Read(ctx, ceil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceil, err)
	}

	title := resp[0][0]

	return &title, nil
}

func (s *SheetsService) SetTitle(ctx context.Context, rowID int64, title string) error {
	ceil := "D" + strconv.FormatInt(rowID, 10)
	if err := s.client.Write(ctx, title, ceil); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceil, err)
	}

	return nil
}

func (s *SheetsService) GetSalaryApplied(ctx context.Context, rowID int64) (*dto.SalaryDataDTO, error) {
	ceilAmount := "E" + strconv.FormatInt(rowID, 10)
	respAmount, err := s.client.Read(ctx, ceilAmount, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceilAmount, err)
	}

	ceilCurrency := "G" + strconv.FormatInt(rowID, 10)
	respCurrency, err := s.client.Read(ctx, ceilCurrency, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceilCurrency, err)
	}

	ceilPeriod := "H" + strconv.FormatInt(rowID, 10)
	respPeriod, err := s.client.Read(ctx, ceilPeriod, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceilPeriod, err)
	}

	if len(respAmount) == 0 || len(respAmount[0]) == 0 || respAmount[0][0] == "" &&
		len(respCurrency) == 0 || len(respCurrency[0]) == 0 || respCurrency[0][0] == "" &&
		len(respPeriod) == 0 || len(respPeriod[0]) == 0 || respPeriod[0][0] == "" {
		return nil, nil
	}

	if respAmount[0][0] != "" {
		amountFrom, amountTo, ok := tools.ParseSalaryRange(respAmount[0][0])
		if !ok {
			return nil, fmt.Errorf("failed to parse salary amount value [%s] in row [%d]", respAmount[0][0], rowID)
		}

		return &dto.SalaryDataDTO{
			AmountFrom: tools.ToPtr(amountFrom),
			AmountTo:   tools.ToPtr(amountTo),
			Currency:   respCurrency[0][0],
			Period:     enums.SalaryPeriod(respPeriod[0][0]),
		}, nil
	}

	return &dto.SalaryDataDTO{
		AmountFrom: nil,
		AmountTo:   nil,
		Currency:   respCurrency[0][0],
		Period:     enums.SalaryPeriod(respPeriod[0][0]),
	}, nil
}

func (s *SheetsService) SetSalaryApplied(ctx context.Context, rowID int64, salary *dto.SalaryDataDTO) error {
	if salary == nil {
		salaryApplied, err := s.GetSalaryApplied(ctx, rowID)
		if err != nil {
			return err
		}
		if salaryApplied == nil {
			sheetRange := "E" + strconv.FormatInt(rowID, 10) + ":H" + strconv.FormatInt(rowID, 10)
			if err := s.client.Write(ctx, "", sheetRange); err != nil {
				return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", sheetRange, err)
			}
			return nil
		}

		sheetRange := "E" + strconv.FormatInt(rowID, 10)
		if err := s.client.Write(ctx, "", sheetRange); err != nil {
			return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", sheetRange, err)
		}
	}

	ceilAmount := "E" + strconv.FormatInt(rowID, 10)
	if salary.AmountFrom != nil && salary.AmountTo != nil {
		salaryAmountStr := strconv.FormatFloat(*salary.AmountFrom, 'f', -1, 64) + "-" + strconv.FormatFloat(*salary.AmountTo, 'f', -1, 64)
		if err := s.client.Write(ctx, salaryAmountStr, ceilAmount); err != nil {
			return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceilAmount, err)
		}
	}

	ceilCurrency := "G" + strconv.FormatInt(rowID, 10)
	if err := s.client.Write(ctx, salary.Currency, ceilCurrency); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceilCurrency, err)
	}

	ceilPeriod := "H" + strconv.FormatInt(rowID, 10)
	if err := s.client.Write(ctx, string(salary.Period), ceilPeriod); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceilPeriod, err)
	}

	return nil
}

func (s *SheetsService) GetSalaryProposed(ctx context.Context, rowID int64) (*dto.SalaryDataDTO, error) {
	ceilAmount := "F" + strconv.FormatInt(rowID, 10)
	respAmount, err := s.client.Read(ctx, ceilAmount, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceilAmount, err)
	}

	ceilCurrency := "G" + strconv.FormatInt(rowID, 10)
	respCurrency, err := s.client.Read(ctx, ceilCurrency, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceilCurrency, err)
	}

	ceilPeriod := "H" + strconv.FormatInt(rowID, 10)
	respPeriod, err := s.client.Read(ctx, ceilPeriod, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceilPeriod, err)
	}

	if len(respAmount) == 0 || len(respAmount[0]) == 0 || respAmount[0][0] == "" &&
		len(respCurrency) == 0 || len(respCurrency[0]) == 0 || respCurrency[0][0] == "" &&
		len(respPeriod) == 0 || len(respPeriod[0]) == 0 || respPeriod[0][0] == "" {
		return nil, nil
	}

	if respAmount[0][0] != "" {
		amountFrom, amountTo, ok := tools.ParseSalaryRange(respAmount[0][0])
		if !ok {
			return nil, fmt.Errorf("failed to parse salary amount value [%s] in row [%d]", respAmount[0][0], rowID)
		}

		return &dto.SalaryDataDTO{
			AmountFrom: tools.ToPtr(amountFrom),
			AmountTo:   tools.ToPtr(amountTo),
			Currency:   respCurrency[0][0],
			Period:     enums.SalaryPeriod(respPeriod[0][0]),
		}, nil
	}

	return &dto.SalaryDataDTO{
		AmountFrom: nil,
		AmountTo:   nil,
		Currency:   respCurrency[0][0],
		Period:     enums.SalaryPeriod(respPeriod[0][0]),
	}, nil
}

func (s *SheetsService) SetSalaryProposed(ctx context.Context, rowID int64, salary *dto.SalaryDataDTO) error {
	if salary == nil {
		salaryProposed, err := s.GetSalaryProposed(ctx, rowID)
		if err != nil {
			return err
		}
		if salaryProposed == nil {
			sheetRange := "E" + strconv.FormatInt(rowID, 10) + ":H" + strconv.FormatInt(rowID, 10)
			if err := s.client.Write(ctx, "", sheetRange); err != nil {
				return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", sheetRange, err)
			}
			return nil
		}

		sheetRange := "F" + strconv.FormatInt(rowID, 10)
		if err := s.client.Write(ctx, "", sheetRange); err != nil {
			return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", sheetRange, err)
		}
	}

	ceilAmount := "F" + strconv.FormatInt(rowID, 10)
	if salary.AmountFrom != nil && salary.AmountTo != nil {
		salaryAmountStr := strconv.FormatFloat(*salary.AmountFrom, 'f', -1, 64) + "-" + strconv.FormatFloat(*salary.AmountTo, 'f', -1, 64)
		if err := s.client.Write(ctx, salaryAmountStr, ceilAmount); err != nil {
			return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceilAmount, err)
		}
	}

	ceilCurrency := "G" + strconv.FormatInt(rowID, 10)
	if err := s.client.Write(ctx, salary.Currency, ceilCurrency); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceilCurrency, err)
	}

	ceilPeriod := "H" + strconv.FormatInt(rowID, 10)
	if err := s.client.Write(ctx, string(salary.Period), ceilPeriod); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceilPeriod, err)
	}

	return nil
}

func (s *SheetsService) GetStatus(ctx context.Context, rowID int64) (*enums.ApplicationStatus, error) {
	ceil := "I" + strconv.FormatInt(rowID, 10)
	resp, err := s.client.Read(ctx, ceil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceil, err)
	}

	status := enums.ApplicationStatus(resp[0][0])
	if !status.IsValid() {
		return nil, fmt.Errorf("invalid application status value [%s] in row [%d]", resp[0][0], rowID)
	}

	return &status, nil
}

func (s *SheetsService) SetStatus(ctx context.Context, rowID int64, status enums.ApplicationStatus) error {
	ceil := "I" + strconv.FormatInt(rowID, 10)
	if err := s.client.Write(ctx, string(status), ceil); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceil, err)
	}

	return nil
}

func (s *SheetsService) GetAppliedAt(ctx context.Context, rowID int64) (*time.Time, error) {
	ceil := "J" + strconv.FormatInt(rowID, 10)
	resp, err := s.client.Read(ctx, ceil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceil, err)
	}

	appliedAtStr := resp[0][0]
	appliedAt, err := time.Parse("02/01/2006", appliedAtStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse appliedAt value [%s] in row [%d]: %w", appliedAtStr, rowID, err)
	}

	return &appliedAt, nil
}

func (s *SheetsService) SetAppliedAt(ctx context.Context, rowID int64, appliedAt time.Time) error {
	ceil := "J" + strconv.FormatInt(rowID, 10)
	appliedAtStr := appliedAt.Format("02/01/2006")
	if err := s.client.Write(ctx, appliedAtStr, ceil); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceil, err)
	}

	return nil
}

func (s *SheetsService) GetRespondedAt(ctx context.Context, rowID int64) (*time.Time, error) {
	ceil := "K" + strconv.FormatInt(rowID, 10)
	resp, err := s.client.Read(ctx, ceil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceil, err)
	}

	respondedAtStr := resp[0][0]
	respondedAt, err := time.Parse("02/01/2006", respondedAtStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse respondedAt value [%s] in row [%d]: %w", respondedAtStr, rowID, err)
	}

	return &respondedAt, nil
}

func (s *SheetsService) SetRespondedAt(ctx context.Context, rowID int64, respondedAt *time.Time) error {
	ceil := "K" + strconv.FormatInt(rowID, 10)
	respondedAtStr := respondedAt.Format("02/01/2006")
	if err := s.client.Write(ctx, respondedAtStr, ceil); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceil, err)
	}

	return nil
}

func (s *SheetsService) GetNextFollowUpAt(ctx context.Context, rowID int64) (*time.Time, error) {
	ceil := "L" + strconv.FormatInt(rowID, 10)
	resp, err := s.client.Read(ctx, ceil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceil, err)
	}

	nextFollowUpAtStr := resp[0][0]
	nextFollowUpAt, err := time.Parse("02/01/2006 15:04:05", nextFollowUpAtStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse nextFollowUpAt value [%s] in row [%d]: %w", nextFollowUpAtStr, rowID, err)
	}

	return &nextFollowUpAt, nil
}

func (s *SheetsService) SetNextFollowUpAt(ctx context.Context, rowID int64, nextFollowUpAt *time.Time) error {
	ceil := "L" + strconv.FormatInt(rowID, 10)
	nextFollowUpAtStr := nextFollowUpAt.Format("02/01/2006 15:04:05")
	if err := s.client.Write(ctx, nextFollowUpAtStr, ceil); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceil, err)
	}

	return nil
}

func (s *SheetsService) GetStage(ctx context.Context, rowID int64) (*string, error) {
	ceil := "M" + strconv.FormatInt(rowID, 10)
	resp, err := s.client.Read(ctx, ceil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceil, err)
	}

	stage := resp[0][0]

	return &stage, nil
}

func (s *SheetsService) SetStage(ctx context.Context, rowID int64, stage *int64) error {
	ceil := "M" + strconv.FormatInt(rowID, 10)
	stageStr := ""
	if stage != nil {
		stageStr = strconv.FormatInt(*stage, 10)
	}
	if err := s.client.Write(ctx, stageStr, ceil); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceil, err)
	}

	return nil
}

func (s *SheetsService) GetContacts(ctx context.Context, rowID int64) (*string, error) {
	ceil := "N" + strconv.FormatInt(rowID, 10)
	resp, err := s.client.Read(ctx, ceil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceil, err)
	}

	contacts := resp[0][0]

	return &contacts, nil
}

func (s *SheetsService) SetContacts(ctx context.Context, rowID int64, contacts string) error {
	ceil := "N" + strconv.FormatInt(rowID, 10)
	if err := s.client.Write(ctx, contacts, ceil); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceil, err)
	}

	return nil
}

func (s *SheetsService) GetJobDescription(ctx context.Context, rowID int64) (*string, error) {
	ceil := "O" + strconv.FormatInt(rowID, 10)
	resp, err := s.client.Read(ctx, ceil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceil, err)
	}

	jobDescription := resp[0][0]

	return &jobDescription, nil
}

func (s *SheetsService) SetJobDescription(ctx context.Context, rowID int64, jobDescription string) error {
	ceil := "O" + strconv.FormatInt(rowID, 10)
	if err := s.client.Write(ctx, jobDescription, ceil); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceil, err)
	}

	return nil
}

func (s *SheetsService) GetNotes(ctx context.Context, rowID int64) (*string, error) {
	ceil := "P" + strconv.FormatInt(rowID, 10)
	resp, err := s.client.Read(ctx, ceil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read Google sheet in range [%s]: %w", ceil, err)
	}

	notes := resp[0][0]

	return &notes, nil
}

func (s *SheetsService) SetNotes(ctx context.Context, rowID int64, notes string) error {
	ceil := "P" + strconv.FormatInt(rowID, 10)
	if err := s.client.Write(ctx, notes, ceil); err != nil {
		return fmt.Errorf("failed to write to Google sheet in range [%s]: %w", ceil, err)
	}

	return nil
}
