package models

import (
	"encoding/json"
	"fmt"
	"time"
)

// Product представляет основную структуру товара
type Product struct {
	ProductID      string         `json:"product_id"`
	Name           string         `json:"name"`
	Description    string         `json:"description"`
	Price          Price          `json:"price"`
	Category       string         `json:"category"`
	Brand          string         `json:"brand"`
	Stock          Stock          `json:"stock"`
	SKU            string         `json:"sku"`
	Tags           []string       `json:"tags"`
	Images         []Image        `json:"images"`
	Specifications Specifications `json:"specifications"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
	Index          string         `json:"index"`
	StoreID        string         `json:"store_id"`
}

// Price представляет структуру цены
type Price struct {
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
}

// Stock представляет информацию о наличии товара
type Stock struct {
	Available int `json:"available"`
	Reserved  int `json:"reserved"`
}

// Image представляет информацию об изображении товара
type Image struct {
	URL string `json:"url"`
	Alt string `json:"alt"`
}

// Specifications содержит технические характеристики
type Specifications struct {
	Weight          string `json:"weight"`
	Dimensions      string `json:"dimensions"`
	BatteryLife     string `json:"battery_life"`
	WaterResistance string `json:"water_resistance"`
}

func main() {
	// Пример использования структур
	product := Product{
		ProductID:   "12345",
		Name:        "Умные часы XYZ",
		Description: "Умные часы с функцией мониторинга здоровья, GPS и уведомлениями.",
		Price: Price{
			Amount:   4999.99,
			Currency: "RUB",
		},
		Category: "Электроника",
		Brand:    "XYZ",
		Stock: Stock{
			Available: 150,
			Reserved:  20,
		},
		SKU:  "XYZ-12345",
		Tags: []string{"умные часы", "гаджеты", "технологии"},
		Images: []Image{
			{
				URL: "https://example.com/images/product1.jpg",
				Alt: "Умные часы XYZ - вид спереди",
			},
			{
				URL: "https://example.com/images/product1_side.jpg",
				Alt: "Умные часы XYZ - вид сбоку",
			},
		},
		Specifications: Specifications{
			Weight:          "50g",
			Dimensions:      "42mm x 36mm x 10mm",
			BatteryLife:     "24 hours",
			WaterResistance: "IP68",
		},
		CreatedAt: parseTime("2023-10-01T12:00:00Z"),
		UpdatedAt: parseTime("2023-10-10T15:30:00Z"),
		Index:     "products",
		StoreID:   "store_001",
	}

	// Преобразование в JSON
	jsonData, err := json.MarshalIndent(product, "", "  ")
	if err != nil {
		fmt.Printf("Error marshaling to JSON: %v", err)
		return
	}

	fmt.Println(string(jsonData))
}

// Вспомогательная функция для парсинга времени
func parseTime(timeStr string) time.Time {
	t, err := time.Parse(time.RFC3339, timeStr)
	if err != nil {
		panic(err)
	}
	return t
}
