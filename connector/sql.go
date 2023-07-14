// Package connector is the compatibility layer from AWS redshiftdata to database/sql.
// Based on https://github.com/jackc/pgx/blob/master/stdlib/sql.go
package connector

import (
	"database/sql"
	"database/sql/driver"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
)

const DEFAULT_MAX_ATTEMPTS = 20
const DEFAULT_REFETCH_DURATION = time.Second

type Config struct {
	Region          *string
	MaxAttempts     *int
	RefetchDuration *time.Duration
	Debug           bool
	*redshiftdata.ExecuteStatementInput
}

func GetConnector(config Config) driver.Connector {

	if config.MaxAttempts == nil {
		maxAttempts := DEFAULT_MAX_ATTEMPTS
		config.MaxAttempts = &maxAttempts
	}

	if config.RefetchDuration == nil {
		duration := DEFAULT_REFETCH_DURATION
		config.RefetchDuration = &duration
	}

	connector := &Connector{
		region:          *config.Region,
		maxAttempts:     *config.MaxAttempts,
		refetchDuration: *config.RefetchDuration,
		debug:           config.Debug,
		input:           config.ExecuteStatementInput,
	}

	return connector
}

func OpenDB(config Config) *sql.DB {

	connector := GetConnector(config)

	return sql.OpenDB(connector)
}
