package enums

type MappingSkipOption string

const (
	MappingSkipOptionEmailMisclassified  MappingSkipOption = "email_misclassified"
	MappingSkipOptionApplicationMismatch MappingSkipOption = "application_mismatch"
	MappingSkipOptionOther               MappingSkipOption = "other_reason"
	MappingSkipOptionBack                MappingSkipOption = "back"
)

var mappingSkipOptions = []MappingSkipOption{
	MappingSkipOptionEmailMisclassified,
	MappingSkipOptionApplicationMismatch,
	MappingSkipOptionOther,
	MappingSkipOptionBack,
}

func (s MappingSkipOption) IsValid() bool {
	for _, mappingSkipOption := range mappingSkipOptions {
		if s == mappingSkipOption {
			return true
		}
	}

	return false
}

func GetAllMappingSkipOptions() []MappingSkipOption {
	return mappingSkipOptions
}
