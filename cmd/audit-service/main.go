package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"audit-service/internal/config"
	"audit-service/internal/consumer"
	"audit-service/internal/handler"
	"audit-service/internal/producer"
	"audit-service/internal/replay"
	"audit-service/internal/repository"
	"audit-service/internal/service"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("service failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect to PostgreSQL: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping PostgreSQL: %w", err)
	}

	eventProducer, err := producer.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	if err != nil {
		return fmt.Errorf("connect to Kafka: %w", err)
	}
	defer eventProducer.Close()

	auditRepository := repository.NewAuditRepository(pool)
	auditService := service.NewAuditService(auditRepository, eventProducer)
	statsReplayer, err := replay.New(cfg.KafkaBrokers, cfg.KafkaTopic, auditRepository)
	if err != nil {
		return fmt.Errorf("connect Kafka replay consumer: %w", err)
	}
	defer statsReplayer.Close()
	httpHandler := handler.New(auditService, statsReplayer, log)

	analyticsConsumer, err := consumer.New(
		consumer.Config{
			Brokers:           cfg.KafkaBrokers,
			GroupID:           cfg.KafkaGroupID,
			Topic:             cfg.KafkaTopic,
			BatchSize:         cfg.KafkaBatchSize,
			CommitInterval:    cfg.CommitInterval,
			AnalyticsInterval: cfg.AnalyticsInterval,
		},
		auditRepository,
		log,
	)
	if err != nil {
		return fmt.Errorf("connect Kafka consumer: %w", err)
	}
	defer analyticsConsumer.Close()

	server := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           httpHandler.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Info("HTTP server started", "address", cfg.HTTPAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("HTTP server failed", "error", err)
			stop()
		}
	}()

	go func() {
		if err := analyticsConsumer.Run(ctx); err != nil {
			log.Error("analytics consumer failed", "error", err)
			stop()
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownPeriod)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown HTTP server: %w", err)
	}
	log.Info("service stopped")
	return nil
}
