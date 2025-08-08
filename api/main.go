package main

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/nats-io/nats.go"
	"github.com/valyala/fasthttp"
	"golang.org/x/net/context"
)

// Pool de conexões otimizado
type ConnectionPools struct {
	nats  *nats.Conn
	redis *redis.Client
	mu    sync.RWMutex
}

var pools *ConnectionPools

// Response structs
type ErrorResponse struct {
	Error string `json:"error"`
}

type SuccessResponse struct {
	Status string `json:"status"`
}

type PaymentSummary struct {
	TotalRequests int64 `json:"totalRequests"`
	TotalAmount   int64 `json:"totalAmount"`
	TotalFee      int64 `json:"totalFee"`
}

type SummaryResponse struct {
	Default  PaymentSummary `json:"default"`
	Fallback PaymentSummary `json:"fallback"`
}

type PaymentData struct {
	Amount int64 `json:"amount"`
	Fee    int64 `json:"fee"`
}

// Regex pré-compilado para performance
var isoDateRegex = regexp.MustCompile(`(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{3}))?`)

// Strings de resposta pré-serializadas para evitar encoding repetido
var (
	acceptedResponse = []byte(`{"status":"accepted"}`)
	purgedResponse   = []byte(`{"status":"purged"}`)
	emptyBodyError   = []byte(`{"error":"empty body"}`)
	natsError        = []byte(`{"error":"nats unavailable"}`)
	redisError       = []byte(`{"error":"redis unavailable"}`)
	publishError     = []byte(`{"error":"publish failed"}`)
	queryError       = []byte(`{"error":"query failed"}`)
	notFoundError    = []byte(`{"error":"not found"}`)
	healthResponse   = []byte("OK\n")
)

func initPools() {
	pools = &ConnectionPools{}

	// Configuração NATS com retry e otimizações
	natsOpts := []nats.Option{
		nats.ReconnectWait(100 * time.Millisecond),
		nats.MaxReconnects(-1),
		nats.PingInterval(10 * time.Second),
		nats.MaxPingsOut(3),
		nats.ReconnectBufSize(8 * 1024 * 1024),
		nats.SubChanLen(64 * 1024),
	}

	var err error
	pools.nats, err = nats.Connect("nats:4222", natsOpts...)
	if err != nil {
		log.Fatalf("Failed to connect to NATS: %v", err)
	}

	// Configuração Redis com pool otimizado
	pools.redis = redis.NewClient(&redis.Options{
		Addr:            "redis-db:6379",
		MaxRetries:      3,
		MinRetryBackoff: 8 * time.Millisecond,
		MaxRetryBackoff: 50 * time.Millisecond,
		DialTimeout:     200 * time.Millisecond,
		ReadTimeout:     200 * time.Millisecond,
		WriteTimeout:    200 * time.Millisecond,
		PoolSize:        100,
		MinIdleConns:    10,
		MaxConnAge:      60 * time.Second,
		PoolTimeout:     200 * time.Millisecond,
		IdleTimeout:     30 * time.Second,
	})

	// Test Redis connection
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := pools.redis.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	log.Println("Connection pools initialized successfully")
}

// Converte ISO 8601 para timestamp Unix em milissegundos
func isoToTimestamp(isoDate string) (string, bool) {
	if isoDate == "" {
		return "", false
	}

	matches := isoDateRegex.FindStringSubmatch(isoDate)
	if matches == nil {
		// Tenta usar como timestamp direto se for número
		if _, err := strconv.ParseInt(isoDate, 10, 64); err == nil {
			return isoDate, true
		}
		return "", false
	}

	year, _ := strconv.Atoi(matches[1])
	month, _ := strconv.Atoi(matches[2])
	day, _ := strconv.Atoi(matches[3])
	hour, _ := strconv.Atoi(matches[4])
	min, _ := strconv.Atoi(matches[5])
	sec, _ := strconv.Atoi(matches[6])

	t := time.Date(year, time.Month(month), day, hour, min, sec, 0, time.UTC)
	timestamp := t.Unix() * 1000

	// Adiciona milissegundos se existirem
	if len(matches) > 7 && matches[7] != "" {
		ms, _ := strconv.Atoi(matches[7])
		timestamp += int64(ms)
	}

	return strconv.FormatInt(timestamp, 10), true
}

// Calcula summary dos dados
func calculateSummary(dataList []string) PaymentSummary {
	var totalAmount, totalFee int64
	var count int64

	for _, item := range dataList {
		if item == "" {
			continue
		}

		var payment PaymentData
		if err := json.Unmarshal([]byte(item), &payment); err == nil {
			totalAmount += payment.Amount
			totalFee += payment.Fee
			count++
		}
	}

	return PaymentSummary{
		TotalRequests: count,
		TotalAmount:   totalAmount,
		TotalFee:      totalFee,
	}
}

// POST /payments - Publica mensagem no NATS
func paymentsHandler(ctx *fasthttp.RequestCtx) {
	// Lê body
	body := ctx.PostBody()
	if len(body) == 0 {
		ctx.SetStatusCode(400)
		ctx.SetContentType("application/json")
		ctx.Write(emptyBodyError)
		return
	}

	// Publica no NATS
	if err := pools.nats.Publish("payments.queue", body); err != nil {
		log.Printf("NATS publish failed: %v", err)
		ctx.SetStatusCode(503)
		ctx.SetContentType("application/json")
		ctx.Write(publishError)
		return
	}

	// Resposta imediata
	ctx.SetStatusCode(201)
	ctx.SetContentType("application/json")
	ctx.Write(acceptedResponse)
}

// GET /payments-summary - Busca dados do Redis e calcula summary
func paymentsSummaryHandler(ctx *fasthttp.RequestCtx) {
	args := ctx.QueryArgs()
	from := string(args.Peek("from"))
	to := string(args.Peek("to"))

	// Converte ou usa valores padrão
	fromScore := "-inf"
	toScore := "+inf"

	if from != "" {
		if ts, ok := isoToTimestamp(from); ok {
			fromScore = ts
		}
	}

	if to != "" {
		if ts, ok := isoToTimestamp(to); ok {
			toScore = ts
		}
	}

	log.Printf("Query range: from=%s to=%s", fromScore, toScore)

	// Context com timeout
	redisCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Busca dados em paralelo
	type result struct {
		data []string
		err  error
	}

	defaultCh := make(chan result, 1)
	fallbackCh := make(chan result, 1)

	// Goroutine para buscar "default"
	go func() {
		data, err := pools.redis.ZRangeByScore(redisCtx, "default", &redis.ZRangeBy{
			Min: fromScore,
			Max: toScore,
		}).Result()
		defaultCh <- result{data, err}
	}()

	// Goroutine para buscar "fallback"
	go func() {
		data, err := pools.redis.ZRangeByScore(redisCtx, "fallback", &redis.ZRangeBy{
			Min: fromScore,
			Max: toScore,
		}).Result()
		fallbackCh <- result{data, err}
	}()

	// Espera resultados
	defaultResult := <-defaultCh
	fallbackResult := <-fallbackCh

	if defaultResult.err != nil || fallbackResult.err != nil {
		errMsg := "unknown error"
		if defaultResult.err != nil {
			errMsg = defaultResult.err.Error()
		} else if fallbackResult.err != nil {
			errMsg = fallbackResult.err.Error()
		}

		log.Printf("Redis query failed: %v", errMsg)
		ctx.SetStatusCode(500)
		ctx.SetContentType("application/json")
		errorResp, _ := json.Marshal(ErrorResponse{Error: fmt.Sprintf("query failed: %s", errMsg)})
		ctx.Write(errorResp)
		return
	}

	// Calcula summaries
	response := SummaryResponse{
		Default:  calculateSummary(defaultResult.data),
		Fallback: calculateSummary(fallbackResult.data),
	}

	// Serializa resposta
	responseBytes, err := json.Marshal(response)
	if err != nil {
		ctx.SetStatusCode(500)
		ctx.SetContentType("application/json")
		ctx.Write(queryError)
		return
	}

	ctx.SetStatusCode(200)
	ctx.SetContentType("application/json")
	ctx.Write(responseBytes)
}

// POST /purge-payments - Limpa dados do Redis
func purgePaymentsHandler(ctx *fasthttp.RequestCtx) {
	redisCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Deleta as chaves
	if err := pools.redis.Del(redisCtx, "default", "fallback").Err(); err != nil {
		log.Printf("Redis delete failed: %v", err)
		ctx.SetStatusCode(503)
		ctx.SetContentType("application/json")
		ctx.Write(redisError)
		return
	}

	ctx.SetStatusCode(200)
	ctx.SetContentType("application/json")
	ctx.Write(purgedResponse)
}

// Health check
func healthHandler(ctx *fasthttp.RequestCtx) {
	ctx.SetStatusCode(200)
	ctx.SetContentType("text/plain")
	ctx.Write(healthResponse)
}

// Router principal
func router(ctx *fasthttp.RequestCtx) {
	method := string(ctx.Method())
	path := string(ctx.Path())

	switch {
	case method == "POST" && path == "/payments":
		paymentsHandler(ctx)
	case method == "GET" && path == "/payments-summary":
		paymentsSummaryHandler(ctx)
	case method == "POST" && path == "/purge-payments":
		purgePaymentsHandler(ctx)
	case method == "GET" && path == "/health":
		healthHandler(ctx)
	default:
		ctx.SetStatusCode(404)
		ctx.SetContentType("application/json")
		ctx.Write(notFoundError)
	}
}

func main() {
	// Inicializa pools de conexão
	initPools()

	// Configuração do servidor HTTP com otimizações máximas
	server := &fasthttp.Server{
		Handler:                router,
		ReadTimeout:            50 * time.Millisecond,
		WriteTimeout:           50 * time.Millisecond,
		IdleTimeout:            65 * time.Second,
		MaxConnsPerIP:          10000,
		MaxRequestsPerConn:     10000,
		MaxRequestBodySize:     1024, // 1KB como no nginx
		DisableKeepalive:       false,
		TCPKeepalive:           true,
		TCPKeepalivePeriod:     60 * time.Second,
		MaxIdleWorkerDuration:  10 * time.Second,
		Concurrency:            256 * 1024,
		DisableHeaderNamesNormalizing: true,
		NoDefaultServerHeader:  true,
		NoDefaultDate:          true,
		ReduceMemoryUsage:      true,
	}

	log.Println("Starting server on :8080")
	log.Fatal(server.ListenAndServe(":8080"))
}