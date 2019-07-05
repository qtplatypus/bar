package bar

import (
	"fmt"
	"testing"
)

func TestOpen(t *testing.T) {

	db, err := Open(
		"/tmp/test.db",
		&Options{
			VacuumFrequency: 17,
		},
	)

	fmt.Printf("%+v %v\n", db, err)

	err = db.Set(10, []byte("qtplatypus"),PH{})

	fmt.Printf("%v\n", err)
}
