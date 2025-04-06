package shopservice

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/ShvetsovYura/pkafka_final/internal/models"
	"github.com/go-chi/chi/v5"
)

func StartApi(messagesCh chan models.Product) {
	r := chi.NewRouter()
	r.Post("/product", func(w http.ResponseWriter, r *http.Request) {
		var product models.Product
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Error reading request body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		err = json.Unmarshal(body, &product)
		if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
		}
		messagesCh <- product
	})

	http.ListenAndServe(":9082", r)

}
