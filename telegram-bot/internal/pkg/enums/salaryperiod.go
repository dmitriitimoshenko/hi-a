package enums

type SalaryPeriod string

const (
	SalaryPeriodMonthly    SalaryPeriod = "monthly"
	SalaryPeriodYearly     SalaryPeriod = "yearly"
	SalaryPeriodPerProject SalaryPeriod = "per project"
	SalaryPeriodNone       SalaryPeriod = "none"
)

var salaryPeriodValues = []SalaryPeriod{
	SalaryPeriodMonthly,
	SalaryPeriodYearly,
	SalaryPeriodPerProject,
	SalaryPeriodNone,
}

func (s SalaryPeriod) IsValid() bool {
	for _, sp := range salaryPeriodValues {
		if s == sp {
			return true
		}
	}

	return false
}

func SalaryPeriodAll() []SalaryPeriod {
	result := make([]SalaryPeriod, len(salaryPeriodValues))
	copy(result, salaryPeriodValues)

	return result
}
