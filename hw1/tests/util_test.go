package tests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/caarlos0/env/v10"
	"github.com/igoroutine-courses/microservices.ecommerce.tests/cart"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type config struct {
	CartGrpcAddr    string `env:"CART_GRPC_ADDR" envDefault:"localhost:50051"`
	CartGatewayAddr string `env:"CART_GATEWAY_ADDR" envDefault:"localhost:8080"`
	LomsGrpcAddr    string `env:"LOMS_GRPC_ADDR" envDefault:"localhost:50052"`
	LomsGatewayAddr string `env:"LOMS_GATEWAY_ADDR" envDefault:"localhost:8081"`
}

func loadConfig(t *testing.T) *config {
	t.Helper()

	var cfg config
	err := env.Parse(&cfg)
	require.NoError(t, err)

	cfg.CartGatewayAddr = normalizeURL(t, cfg.CartGatewayAddr)
	cfg.LomsGatewayAddr = normalizeURL(t, cfg.LomsGatewayAddr)

	return &cfg
}

func normalizeURL(t *testing.T, u string) string {
	t.Helper()
	return "http://" + strings.TrimLeft(u, "https://")
}

// WaitForCartGateway dirty hack with grpc gateway
func WaitForCartGateway(t *testing.T, baseURL string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	body := []byte(`{"user_id":1}`)
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Cart gateway %s did not become ready in %v", baseURL, timeout)
		default:
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/cart/list", bytes.NewReader(body))

		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)

		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		_ = resp.Body.Close()

		if resp.StatusCode/100 == 2 || resp.StatusCode/100 == 4 {
			return
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func WaitForLomsGateway(t *testing.T, baseURL string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	client := &http.Client{Timeout: 2 * time.Second}
	body := []byte(`{"order_id":1}`)

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("LOMS gateway %s did not become ready in %v", baseURL, timeout)
		default:
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/v1/order/info", bytes.NewReader(body))

		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)

		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		_ = resp.Body.Close()

		if resp.StatusCode/100 == 2 || resp.StatusCode/100 == 4 {
			return
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func waitForServices(t *testing.T, cfg *config, timeout time.Duration) {
	t.Helper()
	WaitForCartGateway(t, cfg.CartGatewayAddr, timeout)
	WaitForLomsGateway(t, cfg.LomsGatewayAddr, timeout)
}

func jsonReq(method, url string, body any) (*http.Response, error) {
	var buf bytes.Buffer

	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, err
		}
	}

	req, err := http.NewRequest(method, url, &buf)

	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")

	return http.DefaultClient.Do(req)
}

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(
		ctx,
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)

	require.NoError(t, err)

	t.Cleanup(func() {})
	return conn
}

func listCart(t *testing.T, client cart.CartClient, userID int64) []*cart.ListCartResponse {
	t.Helper()
	ctx := context.Background()

	result := make([]*cart.ListCartResponse, 0)
	stream, err := client.ListCart(ctx, &cart.ListCartRequest{UserId: userID})
	require.NoError(t, err)

	for {
		resp, err := stream.Recv()

		if err == io.EOF {
			return result
		}

		require.NoError(t, err)
		result = append(result, resp)
	}
}

func withLock(mutex sync.Locker, action func()) {
	if action == nil {
		return
	}

	mutex.Lock()
	defer mutex.Unlock()

	action()
}
