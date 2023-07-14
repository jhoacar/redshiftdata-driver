package connector

import (
	"context"
	"database/sql/driver"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
)

// A Connector represents a driver in a fixed configuration
// and can create any number of equivalent Conns for use
// by multiple goroutines.
//
// A Connector can be passed to sql.OpenDB, to allow drivers
// to implement their own sql.DB constructors, or returned by
// DriverContext's OpenConnector method, to allow drivers
// access to context and to avoid repeated parsing of driver
// configuration.
//
// If a Connector implements io.Closer, the sql package's DB.Close
// method will call Close and return error (if any).
type Connector struct {
	region          string
	maxAttempts     int
	refetchDuration time.Duration
	debug           bool
	input           *redshiftdata.ExecuteStatementInput
}

// Connect returns a connection to the database.
// Connect may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The provided context.Context is for dialing purposes only
// (see net.DialContext) and should not be stored or used for
// other purposes. A default timeout should still be used
// when dialing as a connection pool may call Connect
// asynchronously to any query.
//
// The returned connection is only used by one goroutine at a
// time.
func (conn *Connector) Connect(ctx context.Context) (driver.Conn, error) {

	awsConfig, err := config.LoadDefaultConfig(ctx)

	if err != nil {
		return nil, err
	}

	awsConfig.Region = conn.region

	client := redshiftdata.NewFromConfig(awsConfig)

	return &Conn{
		context:         ctx,
		client:          client,
		input:           conn.input,
		maxAttempts:     conn.maxAttempts,
		refetchDuration: conn.refetchDuration,
		debug:           conn.debug,
	}, nil
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (client *Connector) Driver() driver.Driver {
	return &Driver{
		connector: client,
	}
}
