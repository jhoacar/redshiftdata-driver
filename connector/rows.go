package connector

import (
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"math"

	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
)

// Rows is an iterator over an executed query's results.
type Rows struct {
	connection *Conn
	result     *redshiftdata.GetStatementResultOutput
	currentRow int
	columns    []string
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *Rows) Columns() []string {

	if r.connection.debug {
		log.Println("Rows.Columns()")
	}

	if r.result == nil {
		return nil
	}

	if r.columns != nil {
		return r.columns
	}

	r.columns = make([]string, len(r.result.ColumnMetadata))

	for idx, column := range r.result.ColumnMetadata {
		r.columns[idx] = *column.Name
	}

	if r.connection.debug {
		log.Println(r.columns)
	}

	return r.columns
}

// Close closes the rows iterator.
func (r *Rows) Close() error {

	if r.connection.debug {
		log.Println("Rows.Close()")
	}

	r.result = nil

	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *Rows) Next(dest []driver.Value) error {

	if r.connection.debug {
		log.Printf("Rows.Next( columns = %v )", len(dest))
	}

	if r.result == nil {
		return fmt.Errorf("result is empty, rows iterator is closed")
	}

	if r.currentRow == len(r.result.Records) {
		return io.EOF
	}

	row := r.result.Records[r.currentRow]

	for colIdx, fieldValue := range row {

		columnName := *r.result.ColumnMetadata[colIdx].Name
		if r.connection.debug {
			log.Println(columnName)
		}

		switch fieldValue := fieldValue.(type) {

		case *types.FieldMemberStringValue:

			if r.connection.debug {
				log.Println("string")
			}

			dest[colIdx] = fieldValue.Value

		case *types.FieldMemberLongValue:

			if r.connection.debug {
				log.Println("int64")
			}

			dest[colIdx] = fieldValue.Value

		case *types.FieldMemberBooleanValue:

			if r.connection.debug {
				log.Println("bool")
			}

			dest[colIdx] = fieldValue.Value

		case *types.FieldMemberDoubleValue:

			if r.connection.debug {
				log.Println("float64")
			}

			if math.IsNaN(fieldValue.Value) {
				dest[colIdx] = 0
			} else {
				dest[colIdx] = fieldValue.Value
			}

		case *types.FieldMemberIsNull:

			if r.connection.debug {
				log.Println("nil")
			}

			if r.connection.debug {
				log.Println(columnName)
			}

			dest[colIdx] = nil

		default:
			return fmt.Errorf("unsupported field type %s for column %s", fieldValue, columnName)
		}

		if r.connection.debug {
			log.Println(dest[colIdx])
		}
	}

	r.currentRow++

	return nil
}
