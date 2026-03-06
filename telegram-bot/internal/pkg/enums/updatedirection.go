package enums

type UpdateDirection string

const (
	UpdateDirectionInternal UpdateDirection = "internal"
	UpdateDirectionExternal UpdateDirection = "external"
)

var updateDirectionValues = []UpdateDirection{
	UpdateDirectionInternal,
	UpdateDirectionExternal,
}

func (e UpdateDirection) IsValid() bool {
	for _, updateDirection := range updateDirectionValues {
		if e == updateDirection {
			return true
		}
	}

	return false
}

func UpdateDirectionAll() []UpdateDirection {
	result := make([]UpdateDirection, len(updateDirectionValues))
	copy(result, updateDirectionValues)

	return result
}
