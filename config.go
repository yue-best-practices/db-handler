package db_handler

type DbConfig struct {
	Host     string
	Port     int
	UserName string
	Password string
	DbName   string
	Charset  string
	ShowLog  bool
}
