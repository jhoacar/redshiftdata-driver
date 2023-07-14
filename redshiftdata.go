package redshiftdata_gorm

import (
	"github.com/jhoacar/redshiftdata-driver/connector"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func New(redshiftConfig connector.Config, postgresConfig postgres.Config) gorm.Dialector {

	if postgresConfig.Conn == nil {
		postgresConfig.Conn = connector.OpenDB(redshiftConfig)
	}

	return postgres.New(postgresConfig)
}
