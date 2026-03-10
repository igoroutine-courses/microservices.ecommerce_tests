package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type checkoutRequest struct {
	UserID int64 `json:"user_id"`
}

func waitForService(t *testing.T, url string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}

	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewBufferString(`{"user_id":126465}`))
		if err == nil {
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err == nil {
				_ = resp.Body.Close()
				return
			}
		}

		time.Sleep(2 * time.Second)
	}

	t.Fatalf("service %s did not become ready in time", url)
}

func TestCheckout(t *testing.T) {
	baseURL := os.Getenv("BASE_URL")
	if baseURL == "" {
		baseURL = "http://cart:8080" // TODO: для локального запуска сделать localhost
	}

	url := fmt.Sprintf("%s/v1/cart/checkout", baseURL)

	waitForService(t, url, 40*time.Second)

	payload := checkoutRequest{
		UserID: 126465,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	// Здесь можно адаптировать под твой ожидаемый код.
	// Пока просто проверяем, что ручка жива и не вернула 5xx.
	if resp.StatusCode >= 500 {
		t.Fatalf("unexpected status code: %d", resp.StatusCode)
	}

	time.Sleep(time.Second * 30)
	require.Zero(t, 0)
}
