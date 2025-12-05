package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/app/sheets"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/models"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/services/dto"
	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/tools"
	"github.com/pgvector/pgvector-go"
	"golang.org/x/sync/errgroup"
	"gorm.io/gorm"
)

const KAFKA_TOPIC_ADD_APPLICATION_EMBEDDING = "KAFKA_TOPIC_ADD_APPLICATION_EMBEDDING"

type ApplicationService struct {
	db             *gorm.DB
	logger         *slog.Logger
	kafkaPublisher kafkaPublisher
	sheets         sheetsService
	repository     applicationRepository
	salaryService  salaryService
	safetyPerRow   map[int64]bool
}

func NewApplicationService(
	db *gorm.DB,
	logger *slog.Logger,
	kafkaPublisher kafkaPublisher,
	sheets sheetsService,
	repository applicationRepository,
	salaryService salaryService,
) *ApplicationService {
	return &ApplicationService{
		db:             db,
		logger:         logger,
		kafkaPublisher: kafkaPublisher,
		sheets:         sheets,
		repository:     repository,
		salaryService:  salaryService,
		safetyPerRow:   make(map[int64]bool),
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

	if err := s.SyncFromDTO(ctx, application); err != nil {
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

	stageStrPtr := tools.ToPtr("")
	if applicationDTO.Stage != nil {
		stageStrPtr = tools.ToPtr(strconv.FormatInt(*applicationDTO.Stage, 10))
	}

	employmentType := enums.EmploymentType(applicationDTO.EmploymentType)
	if !employmentType.IsValid() {
		return fmt.Errorf("invalid employmentType of value [%s]", employmentType)
	}
	applicationStatus := enums.ApplicationStatus(applicationDTO.Status)
	if !applicationStatus.IsValid() {
		return fmt.Errorf("invalid applicationStatus of value [%s]", applicationStatus)
	}
	workMode := enums.WorkMode(applicationDTO.WorkMode)
	if !workMode.IsValid() {
		return fmt.Errorf("invalid workMode of value [%s]", workMode)
	}

	if err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		application.AppliedAt = applicationDTO.AppliedAt
		application.Company = applicationDTO.Company
		application.Embedding = &embeddingVector
		application.EmploymentType = employmentType
		application.Meta = tools.ToJSONMap(applicationDTO.Meta)
		application.NextFollowUpAt = applicationDTO.NextFollowUpAt
		application.RespondedAt = applicationDTO.RespondedAt
		application.RowID = applicationDTO.RowID
		application.Stage = stageStrPtr
		application.Status = applicationStatus
		application.Title = applicationDTO.Title
		application.WorkMode = workMode

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

func (s *ApplicationService) SyncFromDTO(ctx context.Context, applicationDTO dto.UpdateApplicationDTO) error {
	applicationRowID := applicationDTO.RowID

	applicationFromSheetDTO, err := s.sheets.GetApplicationFromRow(ctx, applicationRowID)
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
	if applicationDTO.EmploymentType != applicationFromSheetDTO.EmploymentType {
		employmentType := enums.EmploymentType(applicationDTO.EmploymentType)
		group.Go(func() error {
			return s.sheets.SetEmploymentType(groupCtx, rowID, employmentType)
		})
	}
	if applicationDTO.WorkMode != applicationFromSheetDTO.WorkMode {
		workMode := enums.WorkMode(applicationDTO.WorkMode)
		group.Go(func() error {
			return s.sheets.SetWorkMode(groupCtx, rowID, workMode)
		})
	}
	if applicationDTO.Status != applicationFromSheetDTO.Status {
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
			applicationDTO.SalaryApplied.Period != applicationFromSheetDTO.SalaryApplied.Period {
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
			applicationDTO.SalaryProposed.Period != applicationFromSheetDTO.SalaryProposed.Period {
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

func (s *ApplicationService) GetApplicationsDiff(ctx context.Context, startRow int64, endRow int64) ([]dto.ApplicationDiffEntry, *int64, error) {
	if startRow < sheets.FirstRowIDAfterHeader || endRow < sheets.FirstRowIDAfterHeader {
		return nil, nil, fmt.Errorf("row IDs must be greater than or equal to [%d]", sheets.FirstRowIDAfterHeader)
	}

	if endRow < startRow {
		return nil, nil, fmt.Errorf("endRow [%d] must be greater than or equal to startRow [%d]", endRow, startRow)
	}

	sheetApplications, err := s.sheets.GetApplicationsFromRows(ctx, startRow, endRow)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get applications from Google Sheets: %w", err)
	}
	s.logger.Info(
		"[GetApplicationsDiff] sheet application got from remote",
		slog.Int("len", len(sheetApplications)),
		slog.Any("keys", maps.Keys(sheetApplications)),
	)

	const maxWorkers = 2

	var mx sync.Mutex
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxWorkers)

	applicationDiffsSet := make([]dto.ApplicationDiffEntry, 0)

	for rowID, sheetApplication := range sheetApplications {
		g.Go(func() error {
			s.logger.Debug(
				"[GetApplicationsDiff] goroutine started",
				slog.Int("row_id", int(rowID)),
			)

			application, err := s.repository.FindByRowID(gctx, rowID)
			if err != nil {
				return fmt.Errorf("failed to find application by rowID [%d]: %w", rowID, err)
			}
			if application == nil || application.ID == 0 {
				return nil
			}
			s.logger.Debug(
				"[GetApplicationsDiff] application check",
				slog.Any("application", *application),
			)

			applicationDiffs, err := s.getApplicationDiff(application, &sheetApplication)
			if err != nil {
				return fmt.Errorf("failed to get application diff for rowID [%d]: %w", rowID, err)
			}
			s.logger.Debug(
				"[GetApplicationsDiff] getApplicationDiff run",
				slog.Int("applicationDiffs_len", len(applicationDiffs)),
			)

			if len(applicationDiffs) == 0 {
				return nil
			}

			e := dto.ApplicationDiffEntry{
				Differences: applicationDiffs,
				RowID:       rowID,
				Company:     &application.Company,
				RoleTitle:   &application.Title,
				Errors:      []string{},
			}

			mx.Lock()
			applicationDiffsSet = append(applicationDiffsSet, e)
			mx.Unlock()

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, nil, fmt.Errorf("failed to process applications for diff: %w", err)
	}

	rowsChecked := int64(len(sheetApplications))

	return applicationDiffsSet, &rowsChecked, nil
}

func (s *ApplicationService) getApplicationDiff(
	dbApplication *models.Application,
	sheetApplication *dto.SheetApplicationDTO,
) ([]dto.ApplicationDiff, error) {
	if dbApplication == nil {
		return []dto.ApplicationDiff{{
			Field:      "application",
			SheetValue: sheetApplication,
			DBValue:    nil,
			Message:    "Application does not exist in DB",
		}}, nil
	}

	diffs := make([]dto.ApplicationDiff, 0)

	if dbApplication.Company != sheetApplication.Company {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "company",
			SheetValue: sheetApplication.Company,
			DBValue:    dbApplication.Company,
			Message:    fmt.Sprintf("Row %d: Company differs (Sheet: %s, DB: %s)", dbApplication.RowID, sheetApplication.Company, dbApplication.Company),
		})
	}

	if dbApplication.Title != sheetApplication.Title {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "title",
			SheetValue: sheetApplication.Title,
			DBValue:    dbApplication.Title,
			Message:    fmt.Sprintf("Row %d: Title differs (Sheet: %s, DB: %s)", dbApplication.RowID, sheetApplication.Title, dbApplication.Title),
		})
	}

	if dbApplication.EmploymentType != sheetApplication.EmploymentType {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "employment_type",
			SheetValue: sheetApplication.EmploymentType,
			DBValue:    dbApplication.EmploymentType,
			Message:    fmt.Sprintf("Row %d: EmploymentType differs (Sheet: %s, DB: %s)", dbApplication.RowID, sheetApplication.EmploymentType, dbApplication.EmploymentType),
		})
	}

	if dbApplication.WorkMode != sheetApplication.WorkMode {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "work_mode",
			SheetValue: sheetApplication.WorkMode,
			DBValue:    dbApplication.WorkMode,
			Message:    fmt.Sprintf("Row %d: WorkMode differs (Sheet: %s, DB: %s)", dbApplication.RowID, sheetApplication.WorkMode, dbApplication.WorkMode),
		})
	}

	if dbApplication.Status != sheetApplication.Status {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "status",
			SheetValue: sheetApplication.Status,
			DBValue:    dbApplication.Status,
			Message:    fmt.Sprintf("Row %d: Status differs (Sheet: %s, DB: %s)", dbApplication.RowID, sheetApplication.Status, dbApplication.Status),
		})
	}

	if !tools.AreDatesEqual(dbApplication.AppliedAt, sheetApplication.AppliedAt) {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "applied_at",
			SheetValue: sheetApplication.AppliedAt,
			DBValue:    dbApplication.AppliedAt.Format("02/01/2006"),
			Message: fmt.Sprintf(
				"Row %d: AppliedAt differs (Sheet: %s, DB: %s)",
				dbApplication.RowID,
				sheetApplication.AppliedAt.Format("02/01/2006"),
				dbApplication.AppliedAt.Format("02/01/2006"),
			),
		})
	}

	if dbApplication.RespondedAt != nil && sheetApplication.RespondedAt != nil &&
		!tools.AreDatesEqual(*dbApplication.RespondedAt, *sheetApplication.RespondedAt) {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "responded_at",
			SheetValue: sheetApplication.RespondedAt.Format("02/01/2006"),
			DBValue:    dbApplication.RespondedAt.Format("02/01/2006"),
			Message: fmt.Sprintf(
				"Row %d: RespondedAt differs (Sheet: %s, DB: %s)",
				dbApplication.RowID,
				sheetApplication.RespondedAt.Format("02/01/2006"),
				dbApplication.RespondedAt.Format("02/01/2006"),
			),
		})
	} else if (dbApplication.RespondedAt == nil && sheetApplication.RespondedAt != nil) ||
		(dbApplication.RespondedAt != nil && sheetApplication.RespondedAt == nil) {
		var sheetValue, dbValue string
		if sheetApplication.RespondedAt != nil {
			sheetValue = sheetApplication.RespondedAt.Format("02/01/2006")
		} else {
			sheetValue = "nil"
		}
		if dbApplication.RespondedAt != nil {
			dbValue = dbApplication.RespondedAt.Format("02/01/2006")
		} else {
			dbValue = "nil"
		}
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "responded_at",
			SheetValue: sheetValue,
			DBValue:    dbValue,
			Message: fmt.Sprintf(
				"Row %d: RespondedAt differs (Sheet: %s, DB: %s)",
				dbApplication.RowID,
				sheetValue,
				dbValue,
			),
		})
	}

	if dbApplication.NextFollowUpAt != nil && sheetApplication.NextFollowUpAt != nil &&
		!dbApplication.NextFollowUpAt.Truncate(time.Second).Equal(sheetApplication.NextFollowUpAt.Truncate(time.Second)) {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "next_follow_up_at",
			SheetValue: sheetApplication.NextFollowUpAt.Format("02/01/2006 15:04:05"),
			DBValue:    dbApplication.NextFollowUpAt.Format("02/01/2006 15:04:05"),
			Message: fmt.Sprintf(
				"Row %d: NextFollowUpAt differs (Sheet: %s, DB: %s)",
				dbApplication.RowID,
				sheetApplication.NextFollowUpAt.Format("02/01/2006 15:04:05"),
				dbApplication.NextFollowUpAt.Format("02/01/2006 15:04:05"),
			),
		})
	} else if (dbApplication.NextFollowUpAt == nil && sheetApplication.NextFollowUpAt != nil) ||
		(dbApplication.NextFollowUpAt != nil && sheetApplication.NextFollowUpAt == nil) {
		var sheetValue, dbValue string
		if sheetApplication.NextFollowUpAt != nil {
			sheetValue = sheetApplication.NextFollowUpAt.Format("02/01/2006 15:04:05")
		}
		if dbApplication.NextFollowUpAt != nil {
			dbValue = dbApplication.NextFollowUpAt.Format("02/01/2006 15:04:05")
		}
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "next_follow_up_at",
			SheetValue: sheetValue,
			DBValue:    dbValue,
			Message: fmt.Sprintf(
				"Row %d: NextFollowUpAt differs (Sheet: %s, DB: %s)",
				dbApplication.RowID,
				sheetValue,
				dbValue,
			),
		})
	}

	if dbApplication.Stage != nil && sheetApplication.Stage != nil {
		dbStage, err := strconv.ParseInt(
			tools.RemoveLetters(*dbApplication.Stage),
			10,
			64,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to parse stage value [%s] in row [%d]: %w", *dbApplication.Stage, dbApplication.RowID, err)
		}
		if dbStage != *sheetApplication.Stage {
			diffs = append(diffs, dto.ApplicationDiff{
				Field:      "stage",
				SheetValue: *sheetApplication.Stage,
				DBValue:    dbStage,
				Message: fmt.Sprintf(
					"Row %d: Stage differs (Sheet: %d, DB: %d)",
					dbApplication.RowID,
					*sheetApplication.Stage,
					dbStage,
				),
			})
		}
	} else if (dbApplication.Stage == nil && sheetApplication.Stage != nil) ||
		(dbApplication.Stage != nil && sheetApplication.Stage == nil) {
		var sheetValue, dbValue string
		if sheetApplication.Stage != nil {
			sheetValue = strconv.FormatInt(*sheetApplication.Stage, 10)
		} else {
			sheetValue = "nil"
		}
		if dbApplication.Stage != nil {
			dbValue = *dbApplication.Stage
		} else {
			dbValue = "nil"
		}
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "stage",
			SheetValue: sheetValue,
			DBValue:    dbValue,
			Message: fmt.Sprintf(
				"Row %d: Stage differs (Sheet: %s, DB: %s)",
				dbApplication.RowID,
				sheetValue,
				dbValue,
			),
		})
	}

	sheetMeta := sheetApplication.Meta
	dbMeta := make(map[string]string)

	if dbApplication.Meta != nil {
		dbMetaMarshalled, err := dbApplication.Meta.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("failed to marshal dbApplication.Meta for row [%d]: %w", dbApplication.RowID, err)
		}
		if err = json.Unmarshal(dbMetaMarshalled, &dbMeta); err != nil {
			return nil, fmt.Errorf("failed to unmarshal dbApplication.Meta for row [%d]: %w", dbApplication.RowID, err)
		}
	}

	if dbMeta["contacts"] != sheetMeta["contacts"] {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "meta.contacts",
			SheetValue: sheetMeta["contacts"],
			DBValue:    dbMeta["contacts"],
			Message: fmt.Sprintf(
				"Row %d: Meta.Contacts differs (Sheet: %s, DB: %s)",
				dbApplication.RowID,
				sheetMeta["contacts"],
				dbMeta["contacts"],
			),
		})
	}

	if dbMeta["job_description"] != sheetMeta["job_description"] {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "meta.job_description",
			SheetValue: sheetMeta["job_description"],
			DBValue:    dbMeta["job_description"],
			Message: fmt.Sprintf(
				"Row %d: Meta.JobDescription differs (Sheet: %s, DB: %s)",
				dbApplication.RowID,
				sheetMeta["job_description"],
				dbMeta["job_description"],
			),
		})
	}

	if dbMeta["notes"] != sheetMeta["notes"] {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "meta.notes",
			SheetValue: sheetMeta["notes"],
			DBValue:    dbMeta["notes"],
			Message: fmt.Sprintf(
				"Row %d: Meta.Notes differs (Sheet: %s, DB: %s)",
				dbApplication.RowID,
				sheetMeta["notes"],
				dbMeta["notes"],
			),
		})
	}

	var dbAppliedSalaryDTO, dbProposedSalaryDTO, sheetAppliedSalaryDTO, sheetProposedSalaryDTO *dto.SalaryDataDTO
	if dbApplication.SalaryApplied != nil {
		dbAppliedSalaryDTO = &dto.SalaryDataDTO{
			AmountFrom: dbApplication.SalaryApplied.AmountFrom,
			AmountTo:   dbApplication.SalaryApplied.AmountTo,
			Currency:   dbApplication.SalaryApplied.Currency,
			Period:     enums.SalaryPeriod(dbApplication.SalaryApplied.Period),
		}
	}
	if dbApplication.SalaryProposed != nil {
		dbProposedSalaryDTO = &dto.SalaryDataDTO{
			AmountFrom: dbApplication.SalaryProposed.AmountFrom,
			AmountTo:   dbApplication.SalaryProposed.AmountTo,
			Currency:   dbApplication.SalaryProposed.Currency,
			Period:     enums.SalaryPeriod(dbApplication.SalaryProposed.Period),
		}
	}
	if sheetApplication.SalaryApplied != nil {
		sheetAppliedSalaryDTO = &dto.SalaryDataDTO{
			AmountFrom: sheetApplication.SalaryApplied.AmountFrom,
			AmountTo:   sheetApplication.SalaryApplied.AmountTo,
			Currency:   sheetApplication.SalaryApplied.Currency,
			Period:     enums.SalaryPeriod(sheetApplication.SalaryApplied.Period),
		}
	}
	if sheetApplication.SalaryProposed != nil {
		sheetProposedSalaryDTO = &dto.SalaryDataDTO{
			AmountFrom: sheetApplication.SalaryProposed.AmountFrom,
			AmountTo:   sheetApplication.SalaryProposed.AmountTo,
			Currency:   sheetApplication.SalaryProposed.Currency,
			Period:     enums.SalaryPeriod(sheetApplication.SalaryProposed.Period),
		}
	}

	if dbAppliedSalaryDTO != nil && sheetAppliedSalaryDTO != nil &&
		!dbAppliedSalaryDTO.IsEqual(sheetAppliedSalaryDTO) {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "salary_applied",
			SheetValue: sheetAppliedSalaryDTO,
			DBValue:    dbAppliedSalaryDTO,
			Message: fmt.Sprintf(
				"Row %d: SalaryApplied differs (Sheet: %s, DB: %s)",
				dbApplication.RowID,
				sheetAppliedSalaryDTO.ToString(),
				dbAppliedSalaryDTO.ToString(),
			),
		})
	} else if (dbAppliedSalaryDTO == nil && sheetAppliedSalaryDTO != nil) ||
		(dbAppliedSalaryDTO != nil && sheetAppliedSalaryDTO == nil) {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "salary_applied",
			SheetValue: sheetAppliedSalaryDTO,
			DBValue:    dbAppliedSalaryDTO,
			Message: fmt.Sprintf(
				"Row %d: SalaryApplied differs (Sheet: %s, DB: %s)",
				dbApplication.RowID,
				sheetAppliedSalaryDTO.ToString(),
				dbAppliedSalaryDTO.ToString(),
			),
		})
	}

	if dbProposedSalaryDTO != nil && sheetProposedSalaryDTO != nil &&
		!dbProposedSalaryDTO.IsEqual(sheetProposedSalaryDTO) {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "salary_proposed",
			SheetValue: sheetProposedSalaryDTO,
			DBValue:    dbProposedSalaryDTO,
			Message: fmt.Sprintf(
				"Row %d: SalaryProposed differs (Sheet: %+v, DB: %+v)",
				dbApplication.RowID,
				sheetProposedSalaryDTO.ToString(),
				dbProposedSalaryDTO.ToString(),
			),
		})
	} else if (dbProposedSalaryDTO == nil && sheetProposedSalaryDTO != nil) ||
		(dbProposedSalaryDTO != nil && sheetProposedSalaryDTO == nil) {
		diffs = append(diffs, dto.ApplicationDiff{
			Field:      "salary_proposed",
			SheetValue: sheetProposedSalaryDTO,
			DBValue:    dbProposedSalaryDTO,
			Message: fmt.Sprintf(
				"Row %d: SalaryProposed differs (Sheet: %+v, DB: %+v)",
				dbApplication.RowID,
				sheetProposedSalaryDTO.ToString(),
				dbProposedSalaryDTO.ToString(),
			),
		})
	}

	return diffs, nil
}

func (s *ApplicationService) GetMaxRowID(ctx context.Context) (*int64, error) {
	c, err := s.repository.GetMaxRowID(ctx)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (s *ApplicationService) Fetch(ctx context.Context) (int64, int64, error) {
	maxRowID, err := s.GetMaxRowID(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to getMaxRowID: %w", err)
	}
	*maxRowID++

	sheetApplications, err := s.sheets.GetApplicationsFromRows(ctx, *maxRowID, sheets.LastRow)
	if err != nil {
		return 0, 0, fmt.Errorf(
			"failed to GetApplicationsFromRows with params StartRow=%d and LastRow=%d: %w",
			*maxRowID,
			sheets.LastRow,
			err,
		)
	}
	if len(sheetApplications) == 0 {
		return 0, 0, nil
	}

	applicationsSavedAmount, salariesSavedAmount, err := s.mapSheetApplicationToModelAndSaveAndSyncFromDB(ctx, sheetApplications)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to mapSheetApplicationToModel: %w", err)
	}

	return applicationsSavedAmount, salariesSavedAmount, nil
}

func (s *ApplicationService) mapSheetApplicationToModelAndSaveAndSyncFromDB(
	ctx context.Context,
	sheetApplications map[int64]dto.SheetApplicationDTO,
) (int64, int64, error) {
	g, gctx := errgroup.WithContext(ctx)

	var applicationsSavedAmount, salariesSavedAmount int64

	for rowID, sheetApplication := range sheetApplications {
		var dbApplication *models.Application
		if err := s.db.WithContext(ctx).Transaction(
			func(tx *gorm.DB) error {
				var stageStr *string
				if sheetApplication.Stage != nil {
					stageStr = tools.ToPtr(strconv.FormatInt(*sheetApplication.Stage, 10))
				}

				var dbSalaryApplied, dbSalaryProposed *models.Salary

				var salaryAppliedAmountFrom, salaryAppliedAmountTo *float64
				if sheetApplication.SalaryApplied != nil {
					if sheetApplication.SalaryApplied.AmountFrom != nil {
						salaryAppliedAmountFrom = sheetApplication.SalaryApplied.AmountFrom
					}
					if sheetApplication.SalaryApplied.AmountTo != nil {
						salaryAppliedAmountTo = sheetApplication.SalaryApplied.AmountTo
					}
					dbSalaryApplied = &models.Salary{
						AmountFrom: salaryAppliedAmountFrom,
						AmountTo:   salaryAppliedAmountTo,
						Currency:   sheetApplication.SalaryApplied.Currency,
						Period:     sheetApplication.SalaryApplied.Period,
					}
				}

				var salaryProposedAmountFrom, salaryProposedAmountTo *float64
				if sheetApplication.SalaryProposed != nil {
					if sheetApplication.SalaryProposed.AmountFrom != nil {
						salaryProposedAmountFrom = sheetApplication.SalaryProposed.AmountFrom
					}
					if sheetApplication.SalaryProposed.AmountTo != nil {
						salaryProposedAmountTo = sheetApplication.SalaryProposed.AmountTo
					}
					dbSalaryProposed = &models.Salary{
						AmountFrom: salaryProposedAmountFrom,
						AmountTo:   salaryProposedAmountTo,
						Currency:   sheetApplication.SalaryProposed.Currency,
						Period:     sheetApplication.SalaryProposed.Period,
					}
				}

				var dbSalaryAppliedID, dbSalaryProposedID *int64
				if dbSalaryApplied != nil {
					if err := tx.WithContext(ctx).Save(dbSalaryApplied).Error; err != nil {
						return err
					}
					dbSalaryAppliedID = &dbSalaryApplied.ID
					salariesSavedAmount++

				}
				if dbSalaryProposed != nil {
					if err := tx.WithContext(ctx).Save(dbSalaryProposed).Error; err != nil {
						return err
					}
					dbSalaryProposedID = &dbSalaryProposed.ID
					salariesSavedAmount++
				}

				dbApplication = &models.Application{
					Company:          sheetApplication.Company,
					Title:            sheetApplication.Title,
					EmploymentType:   sheetApplication.EmploymentType,
					WorkMode:         sheetApplication.WorkMode,
					Status:           sheetApplication.Status,
					AppliedAt:        sheetApplication.AppliedAt,
					RespondedAt:      sheetApplication.RespondedAt,
					NextFollowUpAt:   sheetApplication.NextFollowUpAt,
					Stage:            stageStr,
					Meta:             tools.ToJSONMap(sheetApplication.Meta),
					RowID:            rowID,
					SalaryAppliedID:  dbSalaryAppliedID,
					SalaryProposedID: dbSalaryProposedID,
				}

				if err := tx.WithContext(ctx).Save(dbApplication).Error; err != nil {
					return err
				}
				applicationsSavedAmount++

				return nil
			},
		); err != nil {
			return 0, 0, fmt.Errorf("failed to exec transaction: %w", err)
		}

		g.Go(func() error {
			kafkaPayload := []byte(
				fmt.Sprintf(
					"COMPANY %s TITLE %s EMPLOYMENT TYPE %s WORK MODE %s META %s",
					dbApplication.Company,
					dbApplication.Title,
					dbApplication.EmploymentType,
					dbApplication.WorkMode,
					dbApplication.Meta,
				),
			)

			err := s.kafkaPublisher.Publish(
				gctx,
				os.Getenv(KAFKA_TOPIC_ADD_APPLICATION_EMBEDDING),
				[]byte(strconv.Itoa(int(dbApplication.ID))),
				kafkaPayload,
			)

			if err != nil {
				return fmt.Errorf(
					"failed to publish to KAFKA_TOPIC_ADD_APPLICATION_EMBEDDING %s: %w",
					KAFKA_TOPIC_ADD_APPLICATION_EMBEDDING,
					err,
				)
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return 0, 0, err
	}

	return applicationsSavedAmount, salariesSavedAmount, nil
}

func (s *ApplicationService) List(
	ctx context.Context,
	applicationStatusInclude []enums.ApplicationStatus,
	applicationStatusExclude []enums.ApplicationStatus,
	isReplyEmailReceived bool,
) ([]*models.Application, error) {
	l, err := s.repository.List(
		ctx,
		applicationStatusInclude,
		applicationStatusExclude,
		isReplyEmailReceived,
	)
	if err != nil {
		return nil, err
	}

	return l, nil
}

func (s *ApplicationService) FindByID(ctx context.Context, id int64) (*models.Application, error) {
	application, err := s.repository.FindByID(ctx, id)
	if err != nil {
		return nil, err
	}

	return application, nil
}

func (s *ApplicationService) SyncFromDB(ctx context.Context, id int64) error {
	dbApplication, err := s.repository.FindByID(ctx, id)
	if err != nil {
		return err
	}
	if dbApplication == nil {
		return fmt.Errorf("no application with id [%d] found in the db", id)
	}
	applicationDTO := dto.UpdateApplicationDTO{}
	if err = applicationDTO.MapModel(dbApplication); err != nil {
		return fmt.Errorf("failed to map: %w", err)
	}

	applicationFromSheetDTO, err := s.sheets.GetApplicationFromRow(ctx, dbApplication.RowID)
	if err != nil {
		return fmt.Errorf("failed to get application from Google Sheets by row ID [%d]: %w", dbApplication.RowID, err)
	}

	if err := s.execSyncFromDB(ctx, applicationDTO, applicationFromSheetDTO); err != nil {
		return fmt.Errorf("failed to SyncFromDB to Google Sheets for row ID [%d]: %w", dbApplication.RowID, err)
	}

	return nil
}

func (s *ApplicationService) SyncFromSheet(ctx context.Context, rowID int64) error {
	applicationFromSheetDTO, err := s.sheets.GetApplicationFromRow(ctx, rowID)
	if err != nil {
		return fmt.Errorf("failed to get application from Google Sheets by row ID [%d]: %w", rowID, err)
	}

	applicationDTO, err := s.mapSheetApplicationDTOtoUpdateApplicationDTO(ctx, rowID, applicationFromSheetDTO)
	if err != nil {
		return fmt.Errorf("failed to mapSheetApplicationDTOtoUpdateApplicationDTO with rowID [%d]", rowID)
	}
	if applicationDTO == nil {
		return fmt.Errorf("failed to mapSheetApplicationDTOtoUpdateApplicationDTO with rowID [%d], result empty", rowID)
	}

	if err = s.Update(ctx, *applicationDTO); err != nil {
		return fmt.Errorf("failed to update application with rowID [%d]: %w", rowID, err)
	}

	return nil
}

func (s *ApplicationService) mapSheetApplicationDTOtoUpdateApplicationDTO(
	ctx context.Context,
	rowID int64,
	applicationFromSheetDTO *dto.SheetApplicationDTO,
) (*dto.UpdateApplicationDTO, error) {
	dbApplication, err := s.repository.FindByRowID(ctx, rowID)
	if err != nil {
		return nil, fmt.Errorf("failed to FindByRowID [%d]: %w", rowID, err)
	}
	if dbApplication == nil {
		return nil, fmt.Errorf("no application with rowID [%d] in the db", rowID)
	}

	var salaryApplied, salaryProposed *dto.UpdateApplicationSalaryDTO
	if applicationFromSheetDTO.SalaryApplied != nil {
		salaryApplied = &dto.UpdateApplicationSalaryDTO{
			AmountFrom: applicationFromSheetDTO.SalaryApplied.AmountFrom,
			AmountTo:   applicationFromSheetDTO.SalaryApplied.AmountTo,
			Currency:   applicationFromSheetDTO.SalaryApplied.Currency,
			Period:     applicationFromSheetDTO.SalaryApplied.Period,
		}
	}
	if applicationFromSheetDTO.SalaryProposed != nil {
		salaryProposed = &dto.UpdateApplicationSalaryDTO{
			AmountFrom: applicationFromSheetDTO.SalaryProposed.AmountFrom,
			AmountTo:   applicationFromSheetDTO.SalaryProposed.AmountTo,
			Currency:   applicationFromSheetDTO.SalaryProposed.Currency,
			Period:     applicationFromSheetDTO.SalaryProposed.Period,
		}
	}

	return &dto.UpdateApplicationDTO{
		ID:             dbApplication.ID,
		RowID:          rowID,
		Company:        applicationFromSheetDTO.Company,
		Title:          applicationFromSheetDTO.Title,
		EmploymentType: applicationFromSheetDTO.EmploymentType,
		WorkMode:       applicationFromSheetDTO.WorkMode,
		Status:         applicationFromSheetDTO.Status,
		AppliedAt:      applicationFromSheetDTO.AppliedAt,
		RespondedAt:    applicationFromSheetDTO.RespondedAt,
		NextFollowUpAt: applicationFromSheetDTO.NextFollowUpAt,
		Stage:          applicationFromSheetDTO.Stage,
		Meta:           applicationFromSheetDTO.Meta,
		Embedding:      tools.EmbeddingToSlice(dbApplication.Embedding),
		SalaryApplied:  salaryApplied,
		SalaryProposed: salaryProposed,
	}, nil
}

func (s *ApplicationService) CleanUpMeetingsInBatches(ctx context.Context, batchSize int64) (int64, error) {
	var updatedCount int64
	page := int64(1)
	forward := true

	for forward {
		paginated, err := s.repository.Paginate(
			ctx,
			dto.PaginationParams{
				Page:     page,
				PageSize: batchSize,
			},
		)
		if err != nil {
			return updatedCount, fmt.Errorf("failed to paginate: %w", err)
		}

		updatedInIterationCount, err := s.cleanUpMeetings(ctx, paginated.Content)
		if err != nil {
			return updatedCount, fmt.Errorf("failed to clean up meetings: %w", err)
		}
		updatedCount += updatedInIterationCount

		forward = paginated.NextPage != nil

		if forward {
			page = *paginated.NextPage
		}
	}

	return updatedCount, nil
}

func (s *ApplicationService) cleanUpMeetings(ctx context.Context, applications []*models.Application) (int64, error) {
	var updatedCount int64

	g, gctx := errgroup.WithContext(ctx)

	for _, application := range applications {
		if application.Status == enums.ApplicationStatusMeeting &&
			application.NextFollowUpAt.Truncate(time.Second).Before(time.Now()) {
			application.Status = enums.ApplicationStatusPending

			if err := s.repository.Save(ctx, application); err != nil {
				return updatedCount, fmt.Errorf("failed to save updated applications")
			}

			updatedCount++

			g.Go(func() error {
				if err := s.SyncFromDB(gctx, application.ID); err != nil {
					return err
				}

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return updatedCount, err
	}

	return updatedCount, nil
}
