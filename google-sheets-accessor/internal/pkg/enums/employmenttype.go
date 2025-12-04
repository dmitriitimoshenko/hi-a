package enums

type EmploymentType string

const (
	EmploymentTypeFullTime EmploymentType = "full-time"
	EmploymentTypePartTime EmploymentType = "part-time"
	EmploymentTypeNone     EmploymentType = "none"
	EmploymentTypeB2B      EmploymentType = "b2b"
	EmploymentTypeProject  EmploymentType = "project"
)

var employmentTypeValues = []EmploymentType{
	EmploymentTypeFullTime,
	EmploymentTypePartTime,
	EmploymentTypeNone,
	EmploymentTypeB2B,
	EmploymentTypeProject,
}

func (e EmploymentType) IsValid() bool {
	for _, et := range employmentTypeValues {
		if e == et {
			return true
		}
	}

	return false
}

func EmploymentTypeAll() []EmploymentType {
	result := make([]EmploymentType, len(employmentTypeValues))
	copy(result, employmentTypeValues)

	return result
}
