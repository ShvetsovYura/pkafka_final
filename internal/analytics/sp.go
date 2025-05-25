package analytics

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/types"
	"github.com/apache/spark-connect-go/v35/spark/sql/utils"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func RunCalc(ctx context.Context, addr string, dataPath string, producer *kafka.Producer) error {
	spark, err := sql.NewSessionBuilder().Remote(addr).Build(ctx)
	if err != nil {
		return fmt.Errorf("failed: %w", err)
	}
	defer utils.WarnOnError(spark.Stop, func(err error) {})

	_, err = spark.Sql(ctx, "SELECT 1")
	if err != nil {
		return fmt.Errorf("connection test failed: %w", err)
	}
	df, _ := spark.Read().
		Format("json").
		Option("header", "true").
		Option("inferSchema", "true").
		Load(dataPath)
	// if err != nil {
	// 	return err
	// }
	err = df.Show(ctx, 100, true)
	if err != nil {
		log.Fatal(err)
	}
	err = df.CreateOrReplaceTempView(ctx, "products")
	if err != nil {
		return err
	}
	sqlResult, err := spark.Sql(ctx, `
		WITH words AS (
			SELECT explode(split(value, '[\\s,.!?;:]+')) as word
			FROM products
			WHERE length(trim(value)) > 0
		),
		filtered_words AS (
			SELECT lower(trim(word)) as clean_word
			FROM words
			WHERE length(trim(word)) > 2
			AND trim(word) NOT IN ('и','в','на','с','по','для','из','у','к','от')
		)
		SELECT 
			clean_word as word,
			count(*) as frequency
		FROM filtered_words
		GROUP BY clean_word
		ORDER BY frequency DESC
		LIMIT 10
	`)
	if err != nil {
		return err
	}
	// sqlResult.Show()

	sendToKafka := func(row types.Row) error {
		userUUID, ok := row.Value("user_uuid").(string)
		if !ok {
			return fmt.Errorf("user_uuid not found or not string")
		}

		value, ok := row.Value("words").(string)
		topic := "user_analytics"
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			},
			Key:   []byte(userUUID),
			Value: []byte(value),
		}, nil)

		return err
	}

	// Итерация по DataFrame
	rows, err := sqlResult.Collect(ctx)
	if err != nil {
		return err
	}
	for _, row := range rows {
		log.Printf("Row: %v", row)
		sendToKafka(row)
	}

	producer.Flush(15 * 1000)
	return nil
}

// func s() {
// 	// Альтернативный вариант с SQL
// 	df.CreateOrReplaceTempView("products")
// 	sqlResult, err := spark.Sql(`
// 		WITH words AS (
// 			SELECT explode(split(value, '[\\s,.!?;:]+')) as word
// 			FROM products
// 			WHERE length(trim(value)) > 0
// 		),
// 		filtered_words AS (
// 			SELECT lower(trim(word)) as clean_word
// 			FROM words
// 			WHERE length(trim(word)) > 2
// 			AND trim(word) NOT IN ('и','в','на','с','по','для','из','у','к','от')
// 		)
// 		SELECT
// 			clean_word as word,
// 			count(*) as frequency
// 		FROM filtered_words
// 		GROUP BY clean_word
// 		ORDER BY frequency DESC
// 		LIMIT 10
// 	`)
// 	if err != nil {
// 		panic(err)
// 	}
// 	sqlResult.Show()
// }

// func ugu() {

// 	// Чтение данных (аналогично предыдущему примеру)
// 	df, err := spark.Read().
// 		Format("csv").
// 		Option("header", "true").
// 		Option("inferSchema", "true").
// 		Load("hdfs://namenode:8020/path/to/messages/message_*.csv")
// 	if err != nil {
// 		panic(err)
// 	}

// 	// 1. Извлекаем отдельные слова из названий товаров
// 	// Разбиваем строку на слова по пробелам и другим разделителям
// 	wordsDF, err := df.WithColumn("word",
// 		client.Explode(client.Split(client.Col("value"), "[\\s,.!?;:]+")))
// 	if err != nil {
// 		panic(err)
// 	}

// 	// 2. Фильтруем стоп-слова и пустые строки
// 	filteredWords, err := wordsDF.
// 		Filter(client.Col("word").NotEqual("")).
// 		Filter(client.Length(client.Col("word")).Gt(2)) // Исключаем короткие слова
// 	if err != nil {
// 		panic(err)
// 	}

// 	// 3. Группируем по словам и считаем частоту
// 	wordCounts, err := filteredWords.
// 		GroupBy("word").
// 		Count().
// 		Sort(client.Desc("count"))
// 	if err != nil {
// 		panic(err)
// 	}

// 	// 4. Выводим топ-10 самых частых слов
// 	wordCounts.Show(10)
// }

// func old() {
// 	df, err := spark.Sql(ctx, "select id from range(100)")
// 	if err != nil {
// 		log.Fatalf("Failed: %s", err)
// 	}

// 	err = df.Writer().Mode("overwrite").
// 		Format("parquet").
// 		Save(ctx, "hdfs://namenode:9000/user/test/data.parquet")

// 	if err != nil {
// 		log.Fatalf("Failed1: %s", err)
// 	}

// 	df, err = spark.Read().Format("parquet").
// 		Load("hdfs://namenode:9000/user/test/data.parquet")
// 	if err != nil {
// 		log.Fatalf("Failed2: %s", err)
// 	}

// 	err = df.Show(ctx, 100, false)
// 	if err != nil {
// 		log.Fatalf("Failed: %s", err)
// 	}
// }
