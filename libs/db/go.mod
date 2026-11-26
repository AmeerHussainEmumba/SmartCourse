module github.com/AmeerHussainEmumba/SmartCourse/libs/db

go 1.26.5

require (
	github.com/jackc/pgx/v5 v5.10.0 // indirect
	gorm.io/driver/postgres v1.6.3
	gorm.io/gorm v1.31.2
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/log v1.47.0 // indirect
	go.opentelemetry.io/otel/metric v1.47.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/text v0.42.0 // indirect
)

require (
	github.com/AmeerHussainEmumba/SmartCourse/libs/config v0.0.0
	go.opentelemetry.io/otel v1.47.0
	go.opentelemetry.io/otel/trace v1.47.0
)

replace github.com/AmeerHussainEmumba/SmartCourse/libs/config => ../config
