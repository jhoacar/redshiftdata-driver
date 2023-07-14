package connector

import "log"

// Tx is a transaction.
type Tx struct {
	connection *Conn
}

func (t *Tx) Commit() error {

	if t.connection.debug {
		log.Println("Tx.Commit()")
	}

	return nil
}
func (t *Tx) Rollback() error {

	if t.connection.debug {
		log.Println("Tx.Rollback()")
	}

	return nil
}
