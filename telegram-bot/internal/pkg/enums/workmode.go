package enums

type WorkMode string

const (
	WorkModeOnSite WorkMode = "on-site"
	WorkModeRemote WorkMode = "remote"
	WorkModeHybrid WorkMode = "hybrid"
	WorkModeNone   WorkMode = "none"
)

var workModeValues = []WorkMode{
	WorkModeOnSite,
	WorkModeRemote,
	WorkModeHybrid,
	WorkModeNone,
}

func (w WorkMode) IsValid() bool {
	for _, wm := range workModeValues {
		if w == wm {
			return true
		}
	}

	return false
}

func WorkModeAll() []WorkMode {
	result := make([]WorkMode, len(workModeValues))
	copy(result, workModeValues)

	return result
}
