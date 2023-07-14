# GORM Redshiftdata Driver

## Quick Start

```go
package main

import (
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/jhoacar/redshiftdata-driver"
	"github.com/jhoacar/redshiftdata-driver/connector"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	Region := os.Getenv("AWS_REGION")
	ARN := os.Getenv("AWS_RDS_SECRET_ARN")
	ClusterID := os.Getenv("AWS_CLUSTER_ID")
	DatabaseName := os.Getenv("AWS_RDS_ANTARES_DB_NAME")
	RefetchDuration := 1 * time.Second
	MaxAttempts := 20
	Debug := false

	db, err := gorm.Open(redshiftdata_gorm.New(connector.Config{
		Region:          &Region,
		MaxAttempts:     &MaxAttempts,
		RefetchDuration: &RefetchDuration,
		Debug:           Debug,
		ExecuteStatementInput: &redshiftdata.ExecuteStatementInput{
			Database:          &DatabaseName,
			ClusterIdentifier: &ClusterID,
			SecretArn:         &ARN,
		},
	}, postgres.Config{}), &gorm.Config{})

	if err != nil {
		log.Fatal(err)
	}

	type User struct {
		AccountId string
	}

	var user User

	db.Model(&User{}).Find(&user)

	log.Println(user)
}
```


Checkout [https://gorm.io](https://gorm.io) for details.
