package connector

import (
	"database/sql/driver"
	"fmt"
)

// Driver is the interface that must be implemented by a database
// driver.
//
// Database drivers may implement DriverContext for access
// to contexts and to parse the name only once for a pool of connections,
// instead of once per connection.
type Driver struct {
	connector *Connector
}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
//
// Open may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The returned connection is only used by one goroutine at a
// time.
func (d *Driver) Open(name string) (driver.Conn, error) {
	return nil, fmt.Errorf("Driver.Open(name string) (driver.Conn, error) is not implemented")
}

// OpenConnector must parse the name in the same format that Driver.Open
// parses the name parameter.
func (d *Driver) OpenConnector(name string) (driver.Connector, error) {
	return nil, fmt.Errorf("Driver.OpenConnector(name string) (driver.Connector, error) is not implemented")
}
