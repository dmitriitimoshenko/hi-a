package models

import (
	"time"

	"github.com/dmitriitimoshenko/hi-a/google-sheets-accessor/internal/pkg/enums"
)

type Salary struct {
	ID                     int64              `gorm:"primaryKey"`
	CreatedAt              time.Time          `gorm:"column:created_at;autoCreateTime"`
	UpdatedAt              time.Time          `gorm:"column:updated_at;autoUpdateTime"`
	AmountFrom             *float64           `gorm:"column:amount_from;type:numeric(12,2)"`
	AmountTo               *float64           `gorm:"column:amount_to;type:numeric(12,2)"`
	Currency               string             `gorm:"column:currency;type:varchar(3);not null"`
	Period                 enums.SalaryPeriod `gorm:"column:period;type:varchar(32);not null"`
	ApplicationsAsApplied  []Application      `gorm:"foreignKey:SalaryAppliedID;references:ID;constraint:OnDelete:SET NULL"`
	ApplicationsAsProposed []Application      `gorm:"foreignKey:SalaryProposedID;references:ID;constraint:OnDelete:SET NULL"`
}

func (Salary) TableName() string {
	return "salary"
}
