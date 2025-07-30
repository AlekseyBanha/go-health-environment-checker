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

type OpenSearchChecker interface {
	PingOpenSearch(ctx context.Context) error
}

type RabbitMQChecker interface {
	PingRabbitMQ(ctx context.Context) error
}

type Logger interface {
	Error(msg string, args ...any)
}

type HealthConfig struct {
	RequiredEnvs      []string
	PostgresChecker   PostgresChecker
	OpenSearchChecker OpenSearchChecker
	RabbitMQChecker   RabbitMQChecker
	Logger            Logger
	ConsumerChecker   func(queueName string) bool
}

type HealthChecker struct {
	config HealthConfig
}

func New(config HealthConfig) *HealthChecker {
	return &HealthChecker{config: config}
}

func (hc *HealthChecker) CheckHandler(w http.ResponseWriter, r *http.Request) {
	healthy := true

	if len(hc.config.RequiredEnvs) > 0 && !hc.checkEnvVariables(hc.config.RequiredEnvs) {
		healthy = false
	}

	if !hc.checkPostgreSQL() {
		healthy = false
	}

	if !hc.checkOpenSearch() {
		healthy = false
	}

	if !hc.checkRabbitMQ() {
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

func (hc *HealthChecker) checkConsumer(queueName string) bool {
	if hc.config.ConsumerChecker == nil {
		return true
	}

	return hc.config.ConsumerChecker(queueName)
}
