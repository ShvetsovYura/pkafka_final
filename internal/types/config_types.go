package types

type ProducerConfig struct {
	BootstrapServers string `yaml:"bootstrap_servers" json:"bootstrap_servers"`
	Topic            string `yaml:"topic" json:"topic"`
	SecurityProtocol string `yaml:"security_protocol" json:"security_protocol"`
	CACertLocation   string `yaml:"ca_cert_location" json:"ca_cert_location"`
	CertLocation     string `yaml:"cert_location" json:"cert_location"`
	CertKeyLocation  string `yaml:"cert_key_location" json:"cert_key_location"`
	SASLMechanism    string `yaml:"sasl_mechanism" json:"sasl_mechanism"`
	SASLUsername     string `yaml:"sasl_username" json:"sasl_username"`
	SASLPassword     string `yaml:"sasl_password" json:"sasl_password"`
	Acks             string `yaml:"acks" json:"acks"`
	EnableCertVerify bool   `yaml:"enable_cert_verify" json:"enable_cert_verify"`
	ClientID         string `yaml:"client_id" json:"client_id"`
}

type SchemaRegistryConfig struct {
	URL string `yaml:"url" json:"url"`
}

type WebAPIConfig struct {
	Listen string `yaml:"listen" json:"listen"`
}

type ShopAPIConfig struct {
	Producer       ProducerConfig       `yaml:"producer" json:"producer"`
	SchemaRegistry SchemaRegistryConfig `yaml:"schema_registry" json:"schema_registry"`
	WebAPI         WebAPIConfig         `yaml:"webapi" json:"webapi"`
}
