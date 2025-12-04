package enums

type DiffUpdateDirection string

const (
	DiffUpdateDirectionInternal DiffUpdateDirection = "internal"
	DiffUpdateDirectionExternal DiffUpdateDirection = "external"
)

var diffUpdateDirectionValues = []DiffUpdateDirection{
	DiffUpdateDirectionInternal,
	DiffUpdateDirectionExternal,
}

func (d DiffUpdateDirection) IsValid() bool {
	for _, direction := range diffUpdateDirectionValues {
		if d == direction {
			return true
		}
	}

	return false
}

func DiffUpdateDirectionAll() []DiffUpdateDirection {
	result := make([]DiffUpdateDirection, len(diffUpdateDirectionValues))
	copy(result, diffUpdateDirectionValues)

	return result
}
