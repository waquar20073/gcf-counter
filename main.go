package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

type Config struct {
	DBHost     string `json:"db_host"`
	DBPort     int    `json:"db_port"`
	DBUser     string `json:"db_user"`
	DBPassword string `json:"db_password"`
	DBName     string `json:"db_name"`
	Port       int    `json:"port"`
}

type ResponseBody struct {
	VisitCount int    `json:"visit_count"`
	Error      string `json:"error,omitempty"`
}

var db *sql.DB

// Load configuration from a JSON file
func loadConfig(configPath string) (Config, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return Config{}, err
	}
	defer file.Close()

	var config Config
	err = json.NewDecoder(file).Decode(&config)
	return config, err
}

// Increment the sequence count
func handleIncrement(w http.ResponseWriter, r *http.Request) {
	sequenceName := r.URL.Query().Get("sequence_name")
	if sequenceName == "" {
		http.Error(w, "Missing sequence_name parameter", http.StatusBadRequest)
		return
	}

	var id int
	var sequenceCount int

	tx, err := db.Begin()
	if err != nil {
		sendErrorResponse(w, err)
		return
	}

	// Query the sequence by name
	query := "SELECT id, sequence_count FROM website_hit_sequence WHERE sequence_name = ?"
	err = tx.QueryRow(query, sequenceName).Scan(&id, &sequenceCount)

	if err != nil {
		if err == sql.ErrNoRows {
			// Handle case where no sequence was found
			sendNotFoundResponse(w)
		} else {
			// Handle other SQL errors
			tx.Rollback()
			sendErrorResponse(w, err)
		}
		return
	}

	// Increment the sequence count
	newSequenceCount := sequenceCount + 1
	updateQuery := "UPDATE website_hit_sequence SET sequence_count = ? WHERE id = ?"
	_, err = tx.Exec(updateQuery, newSequenceCount, id)
	if err != nil {
		tx.Rollback()
		sendErrorResponse(w, err)
		return
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		sendErrorResponse(w, err)
		return
	}

	// Send success response
	responseBody := ResponseBody{VisitCount: newSequenceCount}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(responseBody)
}

// Send an error response
func sendErrorResponse(w http.ResponseWriter, err error) {
	log.Println("Error:", err)
	responseBody := ResponseBody{
		VisitCount: -1,
		Error:      "Something went wrong!",
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	json.NewEncoder(w).Encode(responseBody)
}

// Send a not-found response for missing sequence
func sendNotFoundResponse(w http.ResponseWriter) {
	responseBody := ResponseBody{
		VisitCount: -1,
		Error:      "sequence not found",
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusNotFound)
	json.NewEncoder(w).Encode(responseBody)
}

// CORS Middleware
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	// Load configuration
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Open database connection
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.DBUser, config.DBPassword, config.DBHost, config.DBPort, config.DBName)
	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Test the database connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	// Set up routes
	mux := http.NewServeMux()
	mux.HandleFunc("/increment", handleIncrement)

	// Apply CORS middleware
	port := config.Port
	log.Printf("Server is running on port %d...", port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), corsMiddleware(mux)))
}
