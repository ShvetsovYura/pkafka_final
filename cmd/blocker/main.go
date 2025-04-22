package main

import (
	"context"
	"sync"

	blockservice "github.com/ShvetsovYura/pkafka_final/internal/block_service"
)

var s string = "{price=4999.99, name=Умные часы}"

func main() {
	var wg = sync.WaitGroup{}
	blockCh := make(chan *blockservice.BlockItem, 10)
	blocker := blockservice.NewBlocker("block", []string{"kafka.local:9994"}, blockCh, "product_in", "product_out")
	wg.Add(1)
	blocker.Run(context.TODO(), &wg)
	wg.Wait()
}

// product_stream - поступают запросы для добавления продуктов
// product_group - товары после фильтров (проверки на блокировку)
// blocked_products_stream - изменение заблокированных товаров
// blocker-group - содержит заблокированные товары

// var (
// 	brokers                      = []string{"localhost:9092"}
// 	blockStream      goka.Stream = "block"
// 	blockGroup       goka.Group  = "block"
// 	productInStream  goka.Stream = "products_in"
// 	productOutStream goka.Stream = "products_out"

// 	productFilterGroup goka.Group = "product_filter"
// )

// func runBlockerEmmiter() {
// 	e, err := goka.NewEmitter(brokers, blockStream, new(codec.String))
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer e.Finish()
// 	e.EmitSync("blocker", "hoho")
// }

// func runProcessorChangeBlocker() {
// 	g := goka.DefineGroup(blockGroup,
// 		goka.Input(blockStream, new(codec.String), cb),
// 		goka.Persist(new(codec.String)))
// 	p, err := goka.NewProcessor(brokers, g)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	p.Run(context.TODO())
// }

// func cb(ctx goka.Context, msg any) {
// 	k := ctx.Key()
// 	fmt.Println(k)
// 	ctx.SetValue(msg)
// }

// func runFilter() {
// 	g := goka.DefineGroup(productFilterGroup,
// 		goka.Input(productInStream, new(codec.String), filter),
// 		goka.Output(productOutStream, new(codec.String)),
// 		goka.Join(goka.Table(blockGroup), new(codec.String)))
// 	p, err := goka.NewProcessor(brokers, g)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	e := p.Run(context.TODO())
// 	if e != nil {
// 		log.Fatal(e)
// 	}
// }

// func filter(ctx goka.Context, msg any) {
// 	v := ctx.Join(goka.Table(blockGroup))
// 	if v != nil && v.(string) == "lock" {
// 		println("locked")
// 		return

// 	}

// 	ctx.Emit(productOutStream, ctx.Key(), msg)
// }
