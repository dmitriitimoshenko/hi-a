package services

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
	"github.com/pgvector/pgvector-go"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)

type ApplicationService struct {
	db            *gorm.DB
	sheets        sheetsService
	repository    applicationRepository
	salaryService salaryService
	safetyPerRow  map[int64]bool
}

func NewApplicationService(
	db *gorm.DB,
	sheets sheetsService,
	repository applicationRepository,
	salaryService salaryService,
) *ApplicationService {
	return &ApplicationService{
		db:            db,
		sheets:        sheets,
		repository:    repository,
		salaryService: salaryService,
		safetyPerRow:  make(map[int64]bool),
	}
}

func (s *ApplicationService) AddEmbeddingByID(ctx context.Context, id int64, embedding []float32) error {
	application, err := s.repository.FindByID(ctx, id)
	if err != nil {
		return err
	}
	if application == nil {
		return fmt.Errorf("failed to AddEmbeddingByID: no application found by id [%d]", id)
	}

	embeddingVector := pgvector.NewVector(embedding)
	application.Embedding = &embeddingVector

	if err = s.repository.Save(ctx, application); err != nil {
		return err
	}

	return nil
}

func (s *ApplicationService) UpdateAndSync(ctx context.Context, application dto.UpdateApplicationDTO) error {
	if err := s.Update(ctx, application); err != nil {
		return fmt.Errorf("failed to Update application with DTO: \n%+v", application)
	}

	if err := s.SyncFromDB(ctx, application); err != nil {
		return fmt.Errorf("failed to SyncFromDB application with DTO: \n%+v", application)
	}

	return nil
}

func (s *ApplicationService) Update(ctx context.Context, applicationDTO dto.UpdateApplicationDTO) error {
	application, err := s.repository.FindByID(ctx, applicationDTO.ID)
	if err != nil {
		return err
	}
	if application == nil {
		return fmt.Errorf("failed to find an application by id [%d]", applicationDTO.ID)
	}

	embeddingVector := pgvector.NewVector(applicationDTO.Embedding)

	if err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		application.AppliedAt = applicationDTO.AppliedAt
		application.Company = applicationDTO.Company
		application.Embedding = &embeddingVector
		application.EmploymentType = applicationDTO.EmploymentType
		application.Meta = tools.ToJSONMap(applicationDTO.Meta)
		application.NextFollowUpAt = applicationDTO.NextFollowUpAt
		application.RespondedAt = applicationDTO.RespondedAt
		application.RowID = applicationDTO.RowID
		application.Stage = tools.ToPtr(strconv.FormatInt(applicationDTO.Stage, 10))
		application.Status = applicationDTO.Status
		application.Title = applicationDTO.Title
		application.WorkMode = applicationDTO.WorkMode

		if applicationDTO.SalaryApplied != nil {
			salaryApplied := &models.Salary{}
			if application.SalaryAppliedID != nil {
				salaryApplied.ID = *application.SalaryAppliedID
			}
			salaryApplied.AmountFrom = applicationDTO.SalaryApplied.AmountFrom
			salaryApplied.AmountTo = applicationDTO.SalaryApplied.AmountTo
			salaryApplied.Currency = applicationDTO.SalaryApplied.Currency
			salaryApplied.Period = applicationDTO.SalaryApplied.Period
			if err := tx.Save(&salaryApplied).Error; err != nil {
				return err
			}
		}

		if applicationDTO.SalaryProposed != nil {
			salaryProposed := &models.Salary{}
			if application.SalaryProposedID != nil {
				salaryProposed.ID = *application.SalaryProposedID
			}
			salaryProposed.AmountFrom = applicationDTO.SalaryApplied.AmountFrom
			salaryProposed.AmountTo = applicationDTO.SalaryApplied.AmountTo
			salaryProposed.Currency = applicationDTO.SalaryApplied.Currency
			salaryProposed.Period = applicationDTO.SalaryApplied.Period
			if err := tx.Save(&salaryProposed).Error; err != nil {
				return err
			}
		}

		if err := tx.Save(&application).Error; err != nil {
			return err
		}

		return nil
	}); err != nil {
		return fmt.Errorf("failed to update application and salaries in a transaction with application DTO: \n%+v", applicationDTO)
	}

	return nil
}

func (s *ApplicationService) SyncFromDB(ctx context.Context, applicationDTO dto.UpdateApplicationDTO) error {
	applicationRowID := applicationDTO.RowID

	applicationFromSheetDTO, err := s.sheets.GetApplication(ctx, applicationRowID)
	if err != nil {
		return fmt.Errorf("failed to get application from Google Sheets by row ID [%d]: %w", applicationRowID, err)
	}

	if err := s.execSyncFromDB(ctx, applicationDTO, applicationFromSheetDTO); err != nil {
		return fmt.Errorf("failed to SyncFromDB to Google Sheets for row ID [%d]: %w", applicationRowID, err)
	}

	return nil
}

func (s *ApplicationService) execSyncFromDB(
	ctx context.Context,
	applicationDTO dto.UpdateApplicationDTO,
	applicationFromSheetDTO *dto.SheetApplicationDTO,
) error {
	// grant safety for every row in case execSyncFromDB is called concurrently
	for s.safetyPerRow[applicationDTO.RowID] {
		time.Sleep(10 * time.Millisecond)
	}

	s.safetyPerRow[applicationDTO.RowID] = true
	defer delete(s.safetyPerRow, applicationDTO.RowID)

	rowID := applicationDTO.RowID

	group, groupCtx := errgroup.WithContext(ctx)

	if applicationDTO.Company != applicationFromSheetDTO.Company {
		company := applicationDTO.Company
		group.Go(func() error {
			return s.sheets.SetCompany(groupCtx, rowID, company)
		})
	}
	if applicationDTO.Title != applicationFromSheetDTO.Title {
		title := applicationDTO.Title
		group.Go(func() error {
			return s.sheets.SetTitle(groupCtx, rowID, title)
		})
	}
	if applicationDTO.EmploymentType != string(applicationFromSheetDTO.EmploymentType) {
		employmentType := enums.EmploymentType(applicationDTO.EmploymentType)
		group.Go(func() error {
			return s.sheets.SetEmploymentType(groupCtx, rowID, employmentType)
		})
	}
	if applicationDTO.WorkMode != string(applicationFromSheetDTO.WorkMode) {
		workMode := enums.WorkMode(applicationDTO.WorkMode)
		group.Go(func() error {
			return s.sheets.SetWorkMode(groupCtx, rowID, workMode)
		})
	}
	if applicationDTO.Status != string(applicationFromSheetDTO.Status) {
		status := enums.ApplicationStatus(applicationDTO.Status)
		group.Go(func() error {
			return s.sheets.SetStatus(groupCtx, rowID, status)
		})
	}

	if !tools.AreDatesEqual(applicationDTO.AppliedAt, applicationFromSheetDTO.AppliedAt) {
		appliedAt := applicationDTO.AppliedAt
		group.Go(func() error {
			return s.sheets.SetAppliedAt(groupCtx, rowID, appliedAt)
		})
	}

	group.Go(func() error {
		if applicationDTO.RespondedAt != nil && applicationFromSheetDTO.RespondedAt != nil {
			if tools.AreDatesEqual(*applicationDTO.RespondedAt, *applicationFromSheetDTO.RespondedAt) {
				return nil
			}

			return s.sheets.SetRespondedAt(groupCtx, rowID, applicationDTO.RespondedAt)
		}

		if applicationDTO.RespondedAt != nil && applicationFromSheetDTO.RespondedAt == nil {
			return s.sheets.SetRespondedAt(groupCtx, rowID, applicationDTO.RespondedAt)
		}

		if applicationDTO.RespondedAt == nil && applicationFromSheetDTO.RespondedAt != nil {
			return s.sheets.SetRespondedAt(groupCtx, rowID, nil)
		}

		return nil
	})

	group.Go(func() error {
		if applicationDTO.NextFollowUpAt != nil && applicationFromSheetDTO.NextFollowUpAt != nil {
			applicationDTONextFollowUpAt := applicationDTO.NextFollowUpAt.Truncate(time.Second)
			applicationFromSheetDTONextFollowUpAt := applicationFromSheetDTO.NextFollowUpAt.Truncate(time.Second)
			if applicationDTONextFollowUpAt.Equal(applicationFromSheetDTONextFollowUpAt) {
				return nil
			}

			return s.sheets.SetNextFollowUpAt(groupCtx, rowID, applicationDTO.NextFollowUpAt)
		}

		if applicationDTO.NextFollowUpAt != nil && applicationFromSheetDTO.NextFollowUpAt == nil {
			return s.sheets.SetNextFollowUpAt(groupCtx, rowID, applicationDTO.NextFollowUpAt)
		}

		if applicationDTO.NextFollowUpAt == nil && applicationFromSheetDTO.NextFollowUpAt != nil {
			return s.sheets.SetNextFollowUpAt(groupCtx, rowID, nil)
		}

		return nil
	})

	if applicationDTO.Stage != applicationFromSheetDTO.Stage {
		stage := applicationDTO.Stage
		group.Go(func() error {
			return s.sheets.SetStage(groupCtx, rowID, stage)
		})
	}
	if applicationDTO.Meta["contacts"] != applicationFromSheetDTO.Meta["contacts"] {
		contacts := applicationDTO.Meta["contacts"]
		group.Go(func() error {
			return s.sheets.SetContacts(groupCtx, rowID, contacts)
		})
	}
	if applicationDTO.Meta["job_description"] != applicationFromSheetDTO.Meta["job_description"] {
		jobDescription := applicationDTO.Meta["job_description"]
		group.Go(func() error {
			return s.sheets.SetJobDescription(groupCtx, rowID, jobDescription)
		})
	}
	if applicationDTO.Meta["notes"] != applicationFromSheetDTO.Meta["notes"] {
		notes := applicationDTO.Meta["notes"]
		group.Go(func() error {
			return s.sheets.SetNotes(groupCtx, rowID, notes)
		})
	}

	if err := group.Wait(); err != nil {
		return err
	}

	if applicationDTO.SalaryApplied != nil && applicationFromSheetDTO.SalaryApplied != nil {
		if *applicationDTO.SalaryApplied.AmountFrom != *applicationFromSheetDTO.SalaryApplied.AmountFrom ||
			*applicationDTO.SalaryApplied.AmountTo != *applicationFromSheetDTO.SalaryApplied.AmountTo ||
			applicationDTO.SalaryApplied.Currency != applicationFromSheetDTO.SalaryApplied.Currency ||
			applicationDTO.SalaryApplied.Period != string(applicationFromSheetDTO.SalaryApplied.Period) {
			salaryDataDTO := dto.SalaryDataDTO{
				AmountFrom: applicationDTO.SalaryApplied.AmountFrom,
				AmountTo:   applicationDTO.SalaryApplied.AmountTo,
				Currency:   applicationDTO.SalaryApplied.Currency,
				Period:     enums.SalaryPeriod(applicationDTO.SalaryApplied.Period),
			}
			if err := s.sheets.SetSalaryApplied(ctx, rowID, &salaryDataDTO); err != nil {
				return err
			}
		}
	} else if applicationDTO.SalaryApplied != nil && applicationFromSheetDTO.SalaryApplied == nil {
		salaryDataDTO := dto.SalaryDataDTO{
			AmountFrom: applicationDTO.SalaryApplied.AmountFrom,
			AmountTo:   applicationDTO.SalaryApplied.AmountTo,
			Currency:   applicationDTO.SalaryApplied.Currency,
			Period:     enums.SalaryPeriod(applicationDTO.SalaryApplied.Period),
		}
		if err := s.sheets.SetSalaryApplied(ctx, rowID, &salaryDataDTO); err != nil {
			return err
		}
	} else if applicationDTO.SalaryApplied == nil && applicationFromSheetDTO.SalaryApplied != nil {
		if err := s.sheets.SetSalaryApplied(ctx, rowID, nil); err != nil {
			return err
		}
	}

	if applicationDTO.SalaryProposed != nil && applicationFromSheetDTO.SalaryProposed != nil {
		if *applicationDTO.SalaryProposed.AmountFrom != *applicationFromSheetDTO.SalaryProposed.AmountFrom ||
			*applicationDTO.SalaryProposed.AmountTo != *applicationFromSheetDTO.SalaryProposed.AmountTo ||
			applicationDTO.SalaryProposed.Currency != applicationFromSheetDTO.SalaryProposed.Currency ||
			applicationDTO.SalaryProposed.Period != string(applicationFromSheetDTO.SalaryProposed.Period) {
			salaryDataDTO := dto.SalaryDataDTO{
				AmountFrom: applicationDTO.SalaryProposed.AmountFrom,
				AmountTo:   applicationDTO.SalaryProposed.AmountTo,
				Currency:   applicationDTO.SalaryProposed.Currency,
				Period:     enums.SalaryPeriod(applicationDTO.SalaryProposed.Period),
			}
			if err := s.sheets.SetSalaryProposed(ctx, rowID, &salaryDataDTO); err != nil {
				return err
			}
		}
	} else if applicationDTO.SalaryProposed != nil && applicationFromSheetDTO.SalaryProposed == nil {
		salaryDataDTO := dto.SalaryDataDTO{
			AmountFrom: applicationDTO.SalaryProposed.AmountFrom,
			AmountTo:   applicationDTO.SalaryProposed.AmountTo,
			Currency:   applicationDTO.SalaryProposed.Currency,
			Period:     enums.SalaryPeriod(applicationDTO.SalaryProposed.Period),
		}
		if err := s.sheets.SetSalaryProposed(ctx, rowID, &salaryDataDTO); err != nil {
			return err
		}
	} else if applicationDTO.SalaryProposed == nil && applicationFromSheetDTO.SalaryProposed != nil {
		if err := s.sheets.SetSalaryProposed(ctx, rowID, nil); err != nil {
			return err
		}
	}

	return nil
}
