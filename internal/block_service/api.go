package blockservice

import (
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"github.com/go-chi/chi/v5"
)

type BlockerApi struct {
	address string
	blockCh chan *types.BlockItem
}

func NewBlockerWebapi(address string, blockCh chan *types.BlockItem) *BlockerApi {
	return &BlockerApi{
		address: address,
		blockCh: blockCh,
	}
}

func (b *BlockerApi) Start(ctx context.Context, wg *sync.WaitGroup) {
	r := chi.NewRouter()
	r.Post("/", func(w http.ResponseWriter, r *http.Request) {
		var item types.BlockItem
		if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
			http.Error(w, "Invalid request body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		slog.Info("request to block", slog.Any("productID", item))

		b.blockCh <- &item
	})

	server := &http.Server{
		Addr:    b.address,
		Handler: r,
	}
	defer func() {
		wg.Done()
	}()
	go func() {

		if err := server.ListenAndServe(); err != nil {
			log.Fatal(err)
		}

	}()
	log.Println("Blocker webserver started!~")
	<-ctx.Done()
	log.Println("Shutting down server...")
	closeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := server.Shutdown(closeCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
	log.Println("Server stopped")

}
