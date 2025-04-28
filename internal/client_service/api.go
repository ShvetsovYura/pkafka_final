package clientservice

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"github.com/go-chi/chi/v5"
)

type DbClient interface {
	SearchByName(name string) ([]any, error)
}

type ClientApi struct {
	address string

	dbClient DbClient
}

func NewClientApi(address string, dbClient DbClient) *ClientApi {
	return &ClientApi{
		address: address,

		dbClient: dbClient,
	}
}

func (s *ClientApi) Run(ctx context.Context, wg *sync.WaitGroup, reqCh chan types.UserRequest) {
	r := chi.NewRouter()
	r.Get("/search", func(w http.ResponseWriter, r *http.Request) {
		u, err := url.Parse(r.RequestURI)
		if err != nil {
			fmt.Println("ugi")
		}
		qp, _ := url.ParseQuery(u.RawQuery)
		val := qp.Get("name")
		searchResult, _ := s.dbClient.SearchByName(val)
		prettySource, _ := json.Marshal(searchResult)
		w.Write(prettySource)
	})
	r.Get("/recomendations", func(w http.ResponseWriter, r *http.Request) {
		r.Header.Get("client_id")
	})
	server := &http.Server{
		Addr:    s.address,
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
	slog.Info("ClientWebAPI started!")
	<-ctx.Done()
	log.Println("Shutting down server...")
	closeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := server.Shutdown(closeCtx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}
	log.Println("Server stopped")
}
