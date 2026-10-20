package main

import (
    "log"
    "net/http"
    "os"
    "path/filepath"
)

func main() {
    // Determine the repository root (assumes this binary is executed from the repo root)
    cwd, err := os.Getwd()
    if err != nil {
        log.Fatalf("Failed to get working directory: %v", err)
    }
    // Serve the frontend directory
    frontendPath := filepath.Join(cwd, "frontend")
    if _, err := os.Stat(frontendPath); os.IsNotExist(err) {
        log.Fatalf("Frontend directory does not exist at %s", frontendPath)
    }
    fs := http.FileServer(http.Dir(frontendPath))
    http.Handle("/", fs)
    port := "8080"
    log.Printf("Serving UI at http://localhost:%s/", port)
    if err := http.ListenAndServe(":"+port, nil); err != nil {
        log.Fatalf("Server error: %v", err)
    }
}
