package db_handler

type Condition struct {
	Where  string        //  "user=? and status=?"
	Params []interface{} // [ 1,0]
	Desc   []string
	Asc    []string
	Limit  int
	Offset int
}
