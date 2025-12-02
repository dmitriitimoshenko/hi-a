package enums

type BaseAPIResponseStatus string

const (
	BaseAPIResponseStatusOK    BaseAPIResponseStatus = "ok"
	BaseAPIResponseStatusError BaseAPIResponseStatus = "error"
)

var baseAPIResponseStatusValues = []BaseAPIResponseStatus{
	BaseAPIResponseStatusOK,
	BaseAPIResponseStatusError,
}

func (s BaseAPIResponseStatus) IsValid() bool {
	for _, status := range baseAPIResponseStatusValues {
		if s == status {
			return true
		}
	}

	return false
}

func BaseAPIResponseStatusAll() []BaseAPIResponseStatus {
	result := make([]BaseAPIResponseStatus, len(baseAPIResponseStatusValues))
	copy(result, baseAPIResponseStatusValues)

	return result
}
