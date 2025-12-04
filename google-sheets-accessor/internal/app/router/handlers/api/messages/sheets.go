package messages

type SheetsGetRequest struct {
	SheetPage     string `json:"page"`
	SheetID       string `json:"id"`
	SheetCeilFrom string `json:"ceil_from"`
	SheetCeilTo   string `json:"ceil_to"`
}
type SheetsGetResponse struct{
	Data [][]string `json:"data"`
}