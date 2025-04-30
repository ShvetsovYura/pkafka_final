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

type ConsumerConfig struct {
	BootstrapServers string `yaml:"bootstrap_servers" json:"bootstrap_servers"`
	Topic            string `yaml:"topic" json:"topic"`
	SecurityProtocol string `yaml:"security_protocol" json:"security_protocol"`
	CACertLocation   string `yaml:"ca_cert_location" json:"ca_cert_location"`
	CertLocation     string `yaml:"cert_location" json:"cert_location"`
	CertKeyLocation  string `yaml:"cert_key_location" json:"cert_key_location"`
	SASLMechanism    string `yaml:"sasl_mechanism" json:"sasl_mechanism"`
	SASLUsername     string `yaml:"sasl_username" json:"sasl_username"`
	SASLPassword     string `yaml:"sasl_password" json:"sasl_password"`
	EnableCertVerify bool   `yaml:"enable_cert_verify" json:"enable_cert_verify"`
	ClientID         string `yaml:"client_id" json:"client_id"`
	AutoOffsetReset  string `yaml:"auto_offset_reset" json:"auto_offset_reset"`
	EnableAutoCommit bool   `yaml:"enable_autocommit" json:"enable_autocommit"`
	SessionTimeoutMs string `yaml:"session_timeout_ms" json:"session_timeout_ms"`
	GroupID          string `yaml:"group_id" json:"group_id"`
}

type SchemaRegistryConfig struct {
	URL string `yaml:"url" json:"url"`
}

type WebAPIConfig struct {
	Listen string `yaml:"listen" json:"listen"`
}
type CommonConfig struct {
	QueueSize int `yaml:"queue_size" json:"queue_size"`
}

type Certs struct {
	CaCertLocation  string `yaml:"ca_cert_location" json:"ca_cert_location"`
	CertLocation    string `yaml:"cert_location" json:"cert_location"`
	CertKeyLocation string `yaml:"cert_key_location" json:"cert_key_location"`
}

type Cred struct {
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
}

type BlockerTopics struct {
	BlockerTopic string `yaml:"blocker_topic" json:"blocker_topic"`
	InTopic      string `yaml:"in_topic" json:"in_topic"`
	OutTopic     string `yaml:"out_topic" json:"out_topic"`
}

type ElasticClientConfig struct {
	Addr      string `yaml:"addr" json:"addr"`
	IndexName string `yaml:"index_name" json:"index_name"`
}

type HDFSClientConfig struct {
	Addresses           []string `yaml:"addresses" json:"addresses"`
	User                string   `yaml:"user" json:"user"`
	UseDatanodeHostname bool     `yaml:"used_datanode_hostname" json:"used_datanode_hostname"`
}

type ShopConfig struct {
	Producer       ProducerConfig       `yaml:"producer" json:"producer"`
	SchemaRegistry SchemaRegistryConfig `yaml:"schema_registry" json:"schema_registry"`
	WebAPI         WebAPIConfig         `yaml:"webapi" json:"webapi"`
	Common         CommonConfig         `yaml:"common" json:"common"`
}

type BlockerConfig struct {
	BootstrapServers []string      `yaml:"bootstrap_servers" json:"bootstrap_servers"`
	Topics           BlockerTopics `yaml:"topics" json:"topics"`
	Certs            Certs         `yaml:"certs" json:"certs"`
	Credentials      Cred          `yaml:"cred" json:"cred"`
	WebAPI           WebAPIConfig  `yaml:"webapi" json:"webapi"`
	Common           CommonConfig  `yaml:"common" json:"common"`
}

type ClientAppConfig struct {
	Producer            ProducerConfig      `yaml:"producer" json:"producer"`
	Topic               string              `yaml:"topic" json:"topic"`
	ElasticClientConfig ElasticClientConfig `yaml:"elastic" json:"elastic"`
	WebAPI              WebAPIConfig        `yaml:"webapi" json:"webapi"`
	Common              CommonConfig        `yaml:"common" json:"common"`
}

type AndlyticsAppConfig struct {
	HDFS     HDFSClientConfig `yaml:"hdfs" json:"hdfs"`
	Consumer ConsumerConfig   `yaml:"consumer" json:"consumer"`
}
