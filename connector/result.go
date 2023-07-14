package connector

import (
	"log"

	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
)

// Result is the result of a query execution.
type Result struct {
	connection *Conn
	result     *redshiftdata.GetStatementResultOutput
}

// LastInsertId returns the database's auto-generated ID
// after, for example, an INSERT into a table with primary
// key.
func (r *Result) LastInsertId() (int64, error) {
	if r.connection.debug {
		log.Println("Result.LastInsertId()")
	}
	return r.result.TotalNumRows, nil
}

// RowsAffected returns the number of rows affected by the
// query.
func (r *Result) RowsAffected() (int64, error) {
	if r.connection.debug {
		log.Println("Result.RowsAffected()")
	}
	return r.result.TotalNumRows, nil
}
