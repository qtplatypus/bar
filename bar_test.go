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

	fmt.Printf("DB %+v %v\n", db, err)

	err = db.Set(10, []byte("qtplatypus"),PH{})

	fmt.Printf("\nDB %+v - %v\n", db, err)
	fmt.Printf("\nDB %+v", db.head)
}
