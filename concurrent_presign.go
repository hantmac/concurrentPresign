package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/datafuselabs/databend-go"
	"github.com/google/uuid"
)

func main() {
	db, err := sql.Open("databend", "http://databend:databend@localhost:8000")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				url, err := executePresign(db)
				if err != nil {
					panic(err)
				}
				fmt.Println(url)
			}
		}()
	}

	wg.Wait()
}

func executePresign(db *sql.DB) (string, error) {

	fileName := uuid.NewString()
	startTime := time.Now()
	rows, err := db.Query(fmt.Sprintf("PRESIGN UPLOAD @~/%s.csv", fileName))
	endTime := time.Now()
	fmt.Printf("executePresign took: %d ms\n", endTime.Sub(startTime).Milliseconds())
	if err != nil {
		return "", err
	}
	defer rows.Close()
	for rows.Next() {
		var method string
		var headers string
		var url string
		var err = rows.Scan(&method, &headers, &url)

		if err != nil {
			return "", err
		}
		return url, nil
	}

	return "", fmt.Errorf("no url found")
}
