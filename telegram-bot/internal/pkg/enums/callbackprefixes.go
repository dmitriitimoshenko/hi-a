package enums

type CallbackPrefix string

const (
	CallbackPrefixApplicationDiff CallbackPrefix = "application_diff"
)

var callbackPrefixValues = []CallbackPrefix{
	CallbackPrefixApplicationDiff,
}

func (e CallbackPrefix) IsValid() bool {
	for _, callbackPrefix := range callbackPrefixValues {
		if e == callbackPrefix {
			return true
		}
	}

	return false
}

func CallbackPrefixAll() []CallbackPrefix {
	result := make([]CallbackPrefix, len(callbackPrefixValues))
	copy(result, callbackPrefixValues)

	return result
}
