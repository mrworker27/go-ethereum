package eth

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"golang.org/x/net/proxy"
)

type OnionPool interface {
	SubmitTransaction(signedTx *types.Transaction) error
}

type OnionConnPool struct {
	clients   []*PoolClient
	nextIndex atomic.Uint32
	requestId atomic.Uint32
}

type PoolClient struct {
	Client  *http.Client
	Address *url.URL
}

type OnionConfig struct {
	TorProxy        string
	Addresses       []string
	DialTimeout     time.Duration
	KeepAlive       time.Duration
	RequestTimeout  time.Duration
	IdleConnTimeout time.Duration
	MaxIdleConns    int
}

func DefaultOnionConfig(addresses []string) *OnionConfig {
	return &OnionConfig{
		TorProxy:        "127.0.0.1:9050",
		Addresses:       addresses,
		DialTimeout:     60 * time.Second,
		KeepAlive:       120 * time.Second,
		RequestTimeout:  60 * time.Second,
		MaxIdleConns:    10,
		IdleConnTimeout: 90 * time.Second,
	}
}

func NewOnionConnPool(cfg *OnionConfig) (*OnionConnPool, error) {
	if len(cfg.Addresses) == 0 {
		return nil, errors.New("at least one address required")
	}

	pool := &OnionConnPool{
		clients: make([]*PoolClient, 0, len(cfg.Addresses)),
	}

	valid := 0
	var lastError error = nil
	for _, addr := range cfg.Addresses {
		client, err := createTorClient(cfg, addr)
		if err != nil {
			return nil, fmt.Errorf("failed to create client for %s: %w", addr, err)
		}

		if parsedURL, err := url.Parse("http://" + addr); err != nil {
			lastError = err
			continue
		} else {
			valid += 1

			poolClient := &PoolClient{
				Client:  client,
				Address: parsedURL,
			}
			pool.clients = append(pool.clients, poolClient)
		}
	}

	if valid == 0 {
		return nil, fmt.Errorf("all URLs of onion peers are invalid (last err = %s)", lastError)
	}

	return pool, nil
}

func createTorClient(cfg *OnionConfig, targetAddr string) (*http.Client, error) {
	dialer, err := proxy.SOCKS5("tcp", cfg.TorProxy, nil, &net.Dialer{
		Timeout:   cfg.DialTimeout,
		KeepAlive: cfg.KeepAlive,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create SOCKS5 proxy: %w", err)
	}

	httpTransport := &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.(proxy.ContextDialer).DialContext(ctx, network, targetAddr)
		},
		MaxIdleConns:    cfg.MaxIdleConns,
		IdleConnTimeout: cfg.IdleConnTimeout,
	}

	return &http.Client{
		Transport: httpTransport,
		Timeout:   cfg.RequestTimeout,
	}, nil
}

func (p *OnionConnPool) GetClient() (*PoolClient, error) {
	if len(p.clients) == 0 {
		return nil, errors.New("no clients available")
	}

	index := p.nextIndex.Add(1) % uint32(len(p.clients))
	return p.clients[index], nil
}

func (p *OnionConnPool) DoOnionRequest(req *http.Request, maxRetries int) (*http.Response, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		client, err := p.GetClient()
		if err != nil {
			return nil, err
		}

		req.URL = client.Address
		log.Warn("sending request", "tx", client.Address, "req", *req)
		resp, err := client.Client.Do(req)
		if err == nil {
			return resp, nil
		}

		log.Warn("request error", "err", err)

		lastErr = err

		if attempt < maxRetries {
			time.Sleep(time.Duration(attempt+1) * time.Second)
		}
	}

	return nil, fmt.Errorf("all retry attempts failed: %w", lastErr)
}

func (p *OnionConnPool) Close() {
	for _, client := range p.clients {
		client.Client.CloseIdleConnections()
	}
}

// MOO: rename or reuse!
type SendRawTxRequest struct {
	Jsonrpc string   `json:"jsonrpc"`
	Method  string   `json:"method"`
	Params  []string `json:"params"`
	ID      uint32   `json:"id"`
}

// / MOO: rename or reuse!
type SendRawTxResponse struct {
	Result []string    `json:"result"`
	Error  interface{} `json:"error"`
	ID     uint32      `json:"id"`
}

func TransactionHashStr(signedTx *types.Transaction) (string, error) {
	binary, err := signedTx.MarshalBinary()
	if err != nil {
		return "", err
	}

	return "0x" + hex.EncodeToString(binary), nil
}

func NewSendRawTransactionRequest(signedTx *types.Transaction, id uint32) (SendRawTxRequest, error) {
	hash, err := TransactionHashStr(signedTx)
	if err != nil {
		return SendRawTxRequest{}, err
	}

	request := SendRawTxRequest{
		Jsonrpc: "2.0",
		Method:  "eth_sendRawTransaction",
		ID:      id,
		Params:  []string{hash},
	}

	return request, nil
}

func (o *OnionConnPool) SubmitTransaction(signedTx *types.Transaction) error {
	reqBody, err := NewSendRawTransactionRequest(signedTx, o.requestId.Add(1))
	if err != nil {
		return nil
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil
	}
	req, err := http.NewRequestWithContext(
		context.Background(),
		"POST",
		"", // address is handled by conn pool
		bytes.NewReader(body),
	)
	if err != nil {
		return nil
	}

	req.Header.Set("Content-Type", "application/json")
	resp, err := o.DoOnionRequest(req, 2)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil
	}

	var rpcResp SendRawTxResponse
	if err := json.Unmarshal(respBody, &rpcResp); err != nil {
		return nil
	}

	if rpcResp.Error != nil {
		return nil
	}

	resultHash := rpcResp.Result[0]
	if hash, err := TransactionHashStr(signedTx); err != nil || hash != resultHash {
		if err != nil {
			return err
		}

		// MOO: check!
		return fmt.Errorf("got unexpected hash (sent %s, got %s)", hash, resultHash)
	}

	return nil
}
