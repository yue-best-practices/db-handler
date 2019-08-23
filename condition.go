package db_handler

type Condition struct {
	Where  string        //  "user=? and status=?"
	Params []interface{} // [ 1,0]
}
