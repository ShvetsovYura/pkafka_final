package blockservice

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

type BlockItem struct {
	productId string
	status    string
}

type BlockerApi struct {
	address string
	blockCh chan *BlockItem
}

func NewBlockerWebapi(address string, blockCh chan *BlockItem) *BlockerApi {
	return &BlockerApi{
		address: address,
		blockCh: blockCh,
	}
}

func (b *BlockerApi) Start() {
	r := chi.NewRouter()
	r.Post("/{productID}/block", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "productID")
		blockData := &BlockItem{
			productId: id,
			status:    "blocked",
		}

		b.blockCh <- blockData
	})

	r.Post("/{productID}/unblock", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "productID")
		unblockData := &BlockItem{
			productId: id,
			status:    "unblocked",
		}

		b.blockCh <- unblockData
	})

	http.ListenAndServe(b.address, r)
}
