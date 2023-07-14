package connector

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
)

// Conn is a connection to a database. It is not used concurrently
// by multiple goroutines.
//
// Conn is assumed to be stateful.
type Conn struct {
	context         context.Context
	client          *redshiftdata.Client
	input           *redshiftdata.ExecuteStatementInput
	maxAttempts     int
	refetchDuration time.Duration
	debug           bool
	placeholders    [][]string
}

var PLACEHOLDER_REGEX, _ = regexp.Compile(`=[\s]*(\$[\d]+)`)

// Prepare returns a prepared statement, bound to this connection.
//
// Deprecated: Drivers should implement PrepareContext instead (or additionally).
func (conn *Conn) Prepare(query string) (driver.Stmt, error) {
	if conn.debug {
		log.Printf("Conn.Prepare( %v ) ", query)
	}
	return conn.PrepareContext(conn.context, query)
}

// Prepare returns a prepared statement, bound to this connection.
func (conn *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if conn.debug {
		log.Printf("Conn.PrepareContext( %v , `%v` ) ", ctx, query)
	}

	conn.placeholders = PLACEHOLDER_REGEX.FindAllStringSubmatch(query, -1)

	var output *redshiftdata.ExecuteStatementOutput = nil

	if len(conn.placeholders) == 0 {

		conn.input.Sql = &query

		execution, err := conn.client.ExecuteStatement(ctx, conn.input)

		if err != nil {
			return nil, err
		}

		output = execution
	} else {

		parsed := PLACEHOLDER_REGEX.ReplaceAllString(
			query,
			"=%v",
		)

		conn.input.Sql = &parsed
	}

	return &Stmt{
		connection: conn,
		output:     output,
	}, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
//
// Drivers must ensure all network calls made by Close
// do not block indefinitely (e.g. apply a timeout).
func (conn *Conn) Close() error {
	if conn.debug {
		log.Println("Conn.Close()")
	}
	conn.input.Sql = nil
	return nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (conn *Conn) Begin() (driver.Tx, error) {
	if conn.debug {
		log.Println("Conn.Begin()")
	}

	return nil, fmt.Errorf("deprecated: Drivers should implement ConnBeginTx instead (or additionally)")
}

// BeginTx starts and returns a new transaction.
// If the context is canceled by the user the sql package will
// call Tx.Rollback before discarding and closing the connection.
//
// This must check opts.Isolation to determine if there is a set
// isolation level. If the driver does not support a non-default
// level and one is set or if there is a non-default isolation level
// that is not supported, an error must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only
// value is true to either set the read-only transaction property if supported
// or return an error if it is not supported.
func (conn *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if conn.debug {
		log.Printf("Conn.BeginTx( %v, %v )", ctx, opts)
	}

	return &Tx{
		connection: conn,
	}, nil
}
