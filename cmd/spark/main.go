package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/utils"
)

var (
	remote = flag.String("remote", "sc://localhost:15002",
		"the remote address of Spark Connect server to connect to")
)

func main() {
	flag.Parse()
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote(*remote).Build(ctx)
	if err != nil {
		log.Fatalf("Failed: %s", err)
	}
	defer utils.WarnOnError(spark.Stop, func(err error) {})

	df, err := spark.Sql(ctx, "select id from range(100)")
	if err != nil {
		log.Fatalf("Failed: %s", err)
	}

	df, _ = df.FilterByString(ctx, "id < 10")
	explain, err := df.Explain(ctx, utils.ExplainModeFormatted)
	if err != nil {
		log.Fatalf("Failed: %s", err)
		return
	}
	fmt.Println(explain)
}
