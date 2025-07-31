package healthchecker

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"
)

type PostgresChecker interface {
	PingPostgres(ctx context.Context) error
}

type ClickHouseChecker interface {
	PingClickHouse(ctx context.Context) error
}

type OpenSearchChecker interface {
	PingOpenSearch(ctx context.Context) error
}

type RabbitMQChecker interface {
	PingRabbitMQ(ctx context.Context) error
}

type ConsumerChecker interface {
	IsConsumerHealthy() bool
}

type Logger interface {
	Error(msg string, args ...any)
	Info(msg string, args ...any)
}

type HealthConfig struct {
	RequiredEnvs      []string
	PostgresChecker   PostgresChecker
	OpenSearchChecker OpenSearchChecker
	ClickHouseChecker ClickHouseChecker
	RabbitMQChecker   RabbitMQChecker
	ConsumerMQChecker RabbitMQChecker
	ConsumerChecker   ConsumerChecker
	Logger            Logger
}

type HealthChecker struct {
	config HealthConfig
}

func New(config HealthConfig) *HealthChecker {
	return &HealthChecker{config: config}
}

func Serve(hc *HealthChecker) {
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/health", hc.CheckHandler)

	hc.config.Logger.Info("Starting health check server on :81")
	if err := http.ListenAndServe(":81", healthMux); err != nil {
		hc.config.Logger.Error("Failed to start health check server", slog.Any("error", err))
	}
}

func (hc *HealthChecker) CheckHandler(w http.ResponseWriter, r *http.Request) {
	healthy := true

	if len(hc.config.RequiredEnvs) > 0 && !hc.checkEnvVariables(hc.config.RequiredEnvs) {
		healthy = false
	}

	if !hc.checkPostgreSQL() {
		healthy = false
	}

	if !hc.checkClickHouse() {
		healthy = false
	}

	if !hc.checkOpenSearch() {
		healthy = false
	}

	if !hc.checkRabbitMQ() {
		healthy = false
	}

	if !hc.checkConsumer() {
		healthy = false
	}

	if healthy {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (hc *HealthChecker) checkEnvVariables(requiredEnvs []string) bool {
	for _, env := range requiredEnvs {
		if value := os.Getenv(env); value == "" {
			if hc.config.Logger != nil {
				hc.config.Logger.Error("Missing required environment variable", slog.String("env", env))
			}
			return false
		}
	}
	return true
}

func (hc *HealthChecker) checkClickHouse() bool {
	if hc.config.ClickHouseChecker == nil {
		return true
	}

	return hc.checkClickHouseWithError() == nil
}

func (hc *HealthChecker) checkConsumer() bool {
	if hc.config.ConsumerChecker == nil {
		return true
	}
	if !hc.config.ConsumerChecker.IsConsumerHealthy() {
		hc.config.Logger.Error("consumer is frozen. Heartbeat is not updating more 2 minutes")
		return false
	}

	return true
}

func (hc *HealthChecker) checkClickHouseWithError() error {
	if hc.config.ClickHouseChecker == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := hc.config.ClickHouseChecker.PingClickHouse(ctx); err != nil {
		if hc.config.Logger != nil {
			hc.config.Logger.Error("ClickHouse health check failed", slog.Any("error", err.Error()))
		}
		return err
	}
	return nil
}

func (hc *HealthChecker) checkPostgreSQL() bool {
	if hc.config.PostgresChecker == nil {
		return true
	}

	return hc.checkPostgreSQLWithError() == nil
}

func (hc *HealthChecker) checkPostgreSQLWithError() error {
	if hc.config.PostgresChecker == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := hc.config.PostgresChecker.PingPostgres(ctx); err != nil {
		if hc.config.Logger != nil {
			hc.config.Logger.Error("PostgreSQL health check failed", slog.Any("error", err.Error()))
		}
		return err
	}
	return nil
}

func (hc *HealthChecker) checkOpenSearch() bool {
	if hc.config.OpenSearchChecker == nil {
		return true
	}

	return hc.checkOpenSearchWithError() == nil
}

func (hc *HealthChecker) checkOpenSearchWithError() error {
	if hc.config.OpenSearchChecker == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := hc.config.OpenSearchChecker.PingOpenSearch(ctx); err != nil {
		if hc.config.Logger != nil {
			hc.config.Logger.Error("OpenSearch health check failed", slog.Any("error", err.Error()))
		}
		return err
	}
	return nil
}

func (hc *HealthChecker) checkRabbitMQ() bool {
	return hc.checkRabbitMQWithError() == nil
}

func (hc *HealthChecker) checkRabbitMQWithError() error {
	if hc.config.RabbitMQChecker == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := hc.config.RabbitMQChecker.PingRabbitMQ(ctx); err != nil {
		if hc.config.Logger != nil {
			hc.config.Logger.Error("RabbitMQ  health check failed", slog.Any("error", err.Error()))
		}
		return err
	}
	return nil
}
