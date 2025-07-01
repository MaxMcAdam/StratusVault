package metadata

type Config struct {
	Addr string `json:"addr"`
}

func NewConfig() *Config {
	c := &Config{}

	c.SetDefaults()

	return c
}

func (c *Config) SetDefaults() {
	if c.Addr == "" {
		c.Addr = "127.0.0.1:6379"
	}
}
