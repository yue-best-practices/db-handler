package db_handler

type DbConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	UserName string `json:"user_name"`
	Password string `json:"password"`
	DbName   string `json:"db_name"`
	Charset  string `json:"charset"`
	ShowLog  bool   `json:"show_log"`
}

type RedisConfig struct {
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Password string `json:"password"`
	DB       int    `json:"db"`
	Expire   int64  `json:"expire"`
}
