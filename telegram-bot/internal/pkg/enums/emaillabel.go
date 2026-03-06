package enums

type EmailLabel string

const (
	EmailLabelApplied     EmailLabel = "applied"
	EmailLabelMeetingInv  EmailLabel = "meeting_inv"
	EmailLabelMeetingCrt  EmailLabel = "meeting_crt"
	EmailLabelMeetingUpd  EmailLabel = "meeting_upd"
	EmailLabelMeetingCncl EmailLabel = "meeting_cncl"
	EmailLabelOffer       EmailLabel = "offer"
	EmailLabelDenied      EmailLabel = "denied"
	EmailLabelPending     EmailLabel = "pending"
)

var emailLabelValues = []EmailLabel{
	EmailLabelApplied,
	EmailLabelMeetingInv,
	EmailLabelMeetingCrt,
	EmailLabelMeetingUpd,
	EmailLabelMeetingCncl,
	EmailLabelOffer,
	EmailLabelDenied,
	EmailLabelPending,
}

func (l EmailLabel) IsValid() bool {
	for _, label := range emailLabelValues {
		if l == label {
			return true
		}
	}

	return false
}

func EmailLabelAll() []EmailLabel {
	result := make([]EmailLabel, len(emailLabelValues))
	copy(result, emailLabelValues)

	return result
}
