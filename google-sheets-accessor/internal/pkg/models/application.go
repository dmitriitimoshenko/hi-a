package models

import (
	"time"

	"github.com/pgvector/pgvector-go"
	"gorm.io/datatypes"
)

type Application struct {
	ID                       int                `gorm:"primaryKey"`
	CreatedAt                time.Time          `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt                time.Time          `gorm:"column:updated_at;autoUpdateTime"`
	Company                  string             `gorm:"column:company;size:255;not null"`
	Title                    string             `gorm:"column:title;size:255;not null"`
	EmploymentType           string             `gorm:"column:employment_type;size:64;not null"`
	WorkMode                 string             `gorm:"column:work_mode;size:64;not null"`
	Status                   string             `gorm:"column:status;size:64;not null"`
	AppliedAt                time.Time          `gorm:"column:applied_at;not null"`
	RespondedAt              *time.Time         `gorm:"column:responded_at"`
	NextFollowUpAt           *time.Time         `gorm:"column:next_follow_up_at"`
	Stage                    *string            `gorm:"column:stage;size:64"`
	Meta                     *datatypes.JSONMap `gorm:"column:meta;type:jsonb"`
	Embedding                *pgvector.Vector   `gorm:"column:embedding;type:vector(1536)"`
	RowID                    int                `gorm:"column:row_id;uniqueIndex;not null"`
	AppliedEmailReceived     *time.Time         `gorm:"column:applied_email_received"`
	AppliedEmailID           *int               `gorm:"column:applied_email_id"`
	DeniedEmailReceived      *time.Time         `gorm:"column:denied_email_received"`
	DeniedEmailID            *int               `gorm:"column:denied_email_id"`
	MeetingInvEmailReceived  *time.Time         `gorm:"column:meeting_inv_email_received"`
	MeetingInvEmailID        *int               `gorm:"column:meeting_inv_email_id"`
	MeetingCrtEmailReceived  *time.Time         `gorm:"column:meeting_crt_email_received"`
	MeetingCrtEmailID        *int               `gorm:"column:meeting_crt_email_id"`
	MeetingUpdEmailReceived  *time.Time         `gorm:"column:meeting_upd_email_received"`
	MeetingUpdEmailID        *int               `gorm:"column:meeting_upd_email_id"`
	MeetingCnclEmailReceived *time.Time         `gorm:"column:meeting_cncl_email_received"`
	MeetingCnclEmailID       *int               `gorm:"column:meeting_cncl_email_id"`
	SalaryAppliedID          *int               `gorm:"column:salary_applied_id;index"`
	SalaryProposedID         *int               `gorm:"column:salary_proposed_id;index"`
	SalaryApplied            *Salary            `gorm:"foreignKey:SalaryAppliedID;references:ID;constraint:OnDelete:SET NULL"`
	SalaryProposed           *Salary            `gorm:"foreignKey:SalaryProposedID;references:ID;constraint:OnDelete:SET NULL"`
}

func (Application) TableName() string {
	return "application"
}
