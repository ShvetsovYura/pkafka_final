package types

type ClientCert struct {
	CaCertPath        string
	ClientCertPath    string
	ClientCertKeyPath string
}

type Cred struct {
	Username string
	Password string
}

type BlockerTopics struct {
	BlockerTopic string
	InTopic      string
	OutTopic     string
}

type UserRequest struct {
	UserID  string
	Request string
}

type BlockItem struct {
	ProductId string `json:"product_id"`
	Status    string `json:"status"`
}
