package connector

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
)

// Stmt is a prepared statement. It is bound to a Conn and not
// used by multiple goroutines concurrently.
type Stmt struct {
	connection *Conn
	output     *redshiftdata.ExecuteStatementOutput
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
//
// Drivers must ensure all network calls made by Close
// do not block indefinitely (e.g. apply a timeout).
func (s *Stmt) Close() error {
	if s.connection.debug {
		log.Println("Stmt.Close()")
	}
	s.output = nil
	return nil
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *Stmt) NumInput() int {
	if s.connection.debug {
		log.Println("Stmt.NumInput()")
	}
	return len(s.connection.placeholders)
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("deprecated: Drivers should implement StmtExecContext instead (or additionally)")
}

// ExecContext executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// ExecContext must honor the context timeout and return when it is canceled.
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {

	if s.connection.debug {
		log.Printf("Stmt.ExecContext( %v , %v )", ctx, args)
	}

	values := make([]any, len(args))

	for idx, arg := range args {

		value := arg.Value

		switch value.(type) {
		case string:
			values[idx] = fmt.Sprintf("'%s'", value)
		default:
			values[idx] = value
		}
	}

	sqlParsed := fmt.Sprintf(*s.connection.input.Sql, values...)

	if s.connection.debug {
		log.Println(sqlParsed)
	}

	s.connection.input.Sql = &sqlParsed

	execution, err := s.connection.client.ExecuteStatement(ctx, s.connection.input)

	if err != nil {
		return nil, err
	}

	s.output = execution

	result, err := s.GetExecutionResult(ctx)

	if err != nil {
		return nil, err
	}

	return &Result{
		connection: s.connection,
		result:     result,
	}, nil
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	if s.connection.debug {
		log.Printf("Stmt.Query( %v )", args)
	}

	return nil, fmt.Errorf("deprecated: Drivers should implement StmtQueryContext instead (or additionally)")
}

// Query executes a query that may return rows, such as a
// SELECT.
// QueryContext must honor the context timeout and return when it is canceled.
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {

	if s.connection.debug {
		log.Printf("Stmt.QueryContext( %v, %v )", ctx, args)
	}

	if s.output == nil {
		return nil, fmt.Errorf("stmt.output is empty, stmt is closed")
	}

	result, err := s.GetExecutionResult(ctx)

	if err != nil {
		return nil, err
	}

	return &Rows{
		connection: s.connection,
		result:     result,
		currentRow: 0,
	}, nil
}

func (s *Stmt) GetExecutionResult(ctx context.Context) (*redshiftdata.GetStatementResultOutput, error) {

	err := s.CheckExecutionStatus(ctx)

	if err != nil {
		return nil, err
	}

	return s.connection.client.GetStatementResult(ctx, &redshiftdata.GetStatementResultInput{
		Id: s.output.Id,
	})

}

func (s *Stmt) CheckExecutionStatus(ctx context.Context) error {

	attempts := 0

	for {

		if attempts >= s.connection.maxAttempts {
			return fmt.Errorf("too many attempts to retrieve statement ID: %s", *s.output.Id)
		}

		statementResult, err := s.connection.client.DescribeStatement(ctx, &redshiftdata.DescribeStatementInput{
			Id: s.output.Id,
		})

		if err != nil {
			return err
		}

		if statementResult.Status == types.StatusStringFinished {
			return nil

		} else if statementResult.Status == types.StatusStringFailed {
			return fmt.Errorf("failed executing statement: %s", *statementResult.Error)
		}

		attempts++

		time.Sleep(s.connection.refetchDuration)
	}
}
