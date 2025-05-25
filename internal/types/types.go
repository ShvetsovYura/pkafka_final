package types

type UserRequest struct {
	UserID  string
	Request string
}

type BlockItem struct {
	ProductId string `json:"product_id"`
	Status    string `json:"status"`
}
