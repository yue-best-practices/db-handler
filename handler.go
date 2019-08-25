package db_handler

import (
	"fmt"
	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"reflect"
)

type DBHandler struct {
	DB        *xorm.Engine
	dbConf    *DbConfig
	redisConf *RedisConfig
	Redis     *redis.Client
}

func generateDSN(conf *DbConfig) string {
	return fmt.Sprintf("%s:%s@(%s:%d)/%s?charset=%s", conf.UserName, conf.Password, conf.Host, conf.Port, conf.DbName, conf.Charset)
}

func createDBConnection(dsn string, showSql bool) (*xorm.Engine, error) {
	db, err := xorm.NewEngine("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if showSql {
		db.ShowSQL(showSql)
	}
	return db, nil
}

func New(dbConf *DbConfig, redisConf *RedisConfig) (*DBHandler, error) {
	if dbConf == nil {
		return nil, fmt.Errorf("dataBase config is nil")
	}
	dsn := generateDSN(dbConf)
	db, err := createDBConnection(dsn, dbConf.ShowLog)
	if err != nil {
		return nil, err
	}
	var client *redis.Client
	if redisConf != nil {
		client = redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", redisConf.Host, redisConf.Port),
			Password: redisConf.Password,
			DB:       redisConf.DB,
		})
		_, err = client.Ping().Result()
		if err != nil {
			return nil, err
		}
	}
	handler := &DBHandler{DB: db, dbConf: dbConf, Redis: client, redisConf: redisConf}
	return handler, nil
}

// find by id
func (db *DBHandler) Get(bean interface{}, name string, id interface{}) (bool, error) {
	return db.GetOne(bean, name, "id", id)
}

func (db *DBHandler) GetOne(bean interface{}, name string, field string, value interface{}) (bool, error) {
	//todo find from redis
	has, err := db.DB.Table(name).Where(fmt.Sprintf("%s=?", field), value).Get(bean)
	//todo save to redis
	return has, err
}

func (db *DBHandler) List(bean interface{}, name string, condition *Condition) error {
	return db.DB.Table(name).Where(condition.Where, condition.Params...).Find(bean)
}

func (db *DBHandler) Save(bean interface{}, name string, idName ...string) error {
	value := reflect.ValueOf(bean)
	if value.Kind() != reflect.Ptr || value.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("struct pointer expected")
	}
	defaultIdName := "Id"
	if idName != nil {
		defaultIdName = idName[0]
	}
	camelId := ToCamelString(defaultIdName)
	snakeId := ToSnakeString(defaultIdName)
	value = value.Elem()
	idValue := fmt.Sprintf("%v", value.FieldByName(camelId))
	var err error
	if idValue == "0" {
		// todo insert
		_, err = db.DB.Table(name).Insert(bean)
		if err != nil {
			return err
		}
	} else {
		// todo update
		_, err = db.DB.Table(name).Where(fmt.Sprintf("%s=?", snakeId), idValue).AllCols().Update(bean)
		if err != nil {
			return err
		}
	}
	idValue = fmt.Sprintf("%v", value.FieldByName(camelId))
	//todo save to redis
	fmt.Printf("===IdValue:%s\n", idValue)
	return err
}

// delete by id
func (db *DBHandler) Del(bean interface{}, name string, id interface{}) error {
	//todo delete from redis
	_, err := db.DB.Table(name).Where("id=?", id).Delete(bean)
	return err
}

// multiGet by column (default is id)
func (db *DBHandler) MultiGet(bean interface{}, name string, idList []interface{}, columnNames ...string) error {
	column := "id"
	if columnNames != nil {
		column = columnNames[0]
	}

	// todo find from redis

	err := db.DB.Table(name).In(column, idList...).Find(bean)
	return err
}

func (db *DBHandler) Count(name string, condition *Condition) (int64, error) {
	return db.DB.Table(name).Where(condition.Where, condition.Params...).Count()
}

func (db *DBHandler) Sum(bean interface{}, name string, columnName string) (float64, error) {
	return db.DB.Table(name).Sum(bean, columnName)
}

func (db *DBHandler) SumInt(bean interface{}, name string, columnName string) (int64, error) {
	return db.DB.Table(name).SumInt(bean, columnName)
}

func (db *DBHandler) Sums(bean interface{}, name string, columnNames ...string) ([]float64, error) {
	return db.DB.Table(name).Sums(bean, columnNames...)
}

func (db *DBHandler) SumsInt(bean interface{}, name string, columnNames ...string) ([]int64, error) {
	return db.DB.Table(name).SumsInt(bean, columnNames...)
}

func (db *DBHandler) Exec(sql string, params ...interface{}) error {
	_, err := db.DB.Exec(sql, params)
	return err
}

func (db *DBHandler) Query(sql string, params ...interface{}) ([]map[string]interface{}, error) {
	return db.DB.QueryInterface(sql, params)
}

func (db *DBHandler) Flush() error {
	if db.Redis != nil {
		return db.Redis.FlushDB().Err()
	}
	return nil
}
