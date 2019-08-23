package db_handler

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"reflect"
)

type DBHandler struct {
	DB *xorm.Engine
}

func generateDSN(conf DbConfig) string {
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

func New(conf DbConfig) (*DBHandler, error) {
	dsn := generateDSN(conf)
	db, err := createDBConnection(dsn, conf.ShowLog)
	if err != nil {
		return nil, err
	}
	handler := &DBHandler{DB: db}
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

//func (db *DBHandler) Update(bean interface{},name string,)  {
//
//}
