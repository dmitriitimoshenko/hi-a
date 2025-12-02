package enums

type ApplicationStatus string

const (
	ApplicationStatusApplied ApplicationStatus = "applied"
	ApplicationStatusMeeting ApplicationStatus = "meeting"
	ApplicationStatusOffer   ApplicationStatus = "offer"
	ApplicationStatusDenied  ApplicationStatus = "denied"
	ApplicationStatusPending ApplicationStatus = "pending"
)

var applicationStatusValues = []ApplicationStatus{
	ApplicationStatusApplied,
	ApplicationStatusMeeting,
	ApplicationStatusOffer,
	ApplicationStatusDenied,
	ApplicationStatusPending,
}

func (s ApplicationStatus) IsValid() bool {
	for _, status := range applicationStatusValues {
		if s == status {
			return true
		}
	}

	return false
}

func ApplicationStatusAll() []ApplicationStatus {
	result := make([]ApplicationStatus, len(applicationStatusValues))
	copy(result, applicationStatusValues)

	return result
}
