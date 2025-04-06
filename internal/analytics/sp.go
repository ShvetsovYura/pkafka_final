package analytics

import (
	"context"
	"log"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/utils"
)

func RunCalc() {
	remote := "sc://localhost:15002"
	ctx := context.Background()
	spark, err := sql.NewSessionBuilder().Remote(remote).Build(ctx)
	if err != nil {
		log.Fatalf("Failed: %s", err)
	}
	defer utils.WarnOnError(spark.Stop, func(err error) {})

	df, err := spark.Sql(ctx, "select id from range(100)")
	if err != nil {
		log.Fatalf("Failed: %s", err)
	}

	err = df.Writer().Mode("overwrite").
		Format("parquet").
		Save(ctx, "hdfs://namenode:9000/user/test/data.parquet")

	if err != nil {
		log.Fatalf("Failed1: %s", err)
	}

	df, err = spark.Read().Format("parquet").
		Load("hdfs://namenode:9000/user/test/data.parquet")
	if err != nil {
		log.Fatalf("Failed2: %s", err)
	}

	err = df.Show(ctx, 100, false)
	if err != nil {
		log.Fatalf("Failed: %s", err)
	}
}
