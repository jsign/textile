package core

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpgrpc"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/semconv"
	"google.golang.org/grpc"
)

// ShutdownFunc shutdowns gracefully all observability components.
type telShutdownFunc func() error

func initTelemetry(otlEndpoint string) (telShutdownFunc, error) {
	ctx := context.Background()

	driver := otlpgrpc.NewDriver(
		otlpgrpc.WithInsecure(),
		otlpgrpc.WithEndpoint(otlEndpoint),
		otlpgrpc.WithDialOption(grpc.WithBlock()),
	)
	exp, err := otlp.NewExporter(ctx, driver)
	if err != nil {
		return nil, fmt.Errorf("creating exporter: %s", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String("hub"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %s", err)
	}

	bsp := sdktrace.NewBatchSpanProcessor(exp)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithConfig(sdktrace.Config{DefaultSampler: sdktrace.AlwaysSample()}),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)

	otel.SetTracerProvider(tracerProvider)

	shutdown := telShutdownFunc(func() error {
		if err := tracerProvider.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown TracerProvider: %s", err)
		}

		return nil
	})

	return shutdown, nil
}
