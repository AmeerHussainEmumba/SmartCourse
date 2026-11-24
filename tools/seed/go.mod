module github.com/AmeerHussainEmumba/SmartCourse/tools/seed

go 1.26.5

require (
	github.com/gin-gonic/gin v1.12.0 // indirect
	github.com/go-redis/redis_rate/v10 v10.0.1 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.10.0 // indirect
	github.com/lib/pq v1.10.9 // indirect
	github.com/redis/go-redis/v9 v9.22.0 // indirect
	go.temporal.io/sdk v1.50.0 // indirect
	golang.org/x/crypto v0.57.0 // indirect
	gorm.io/driver/postgres v1.6.3 // indirect
	gorm.io/gorm v1.31.2 // indirect
)

require (
	github.com/AmeerHussainEmumba/SmartCourse/libs/auth v0.0.0 // indirect
	github.com/AmeerHussainEmumba/SmartCourse/libs/httpx v0.0.0 // indirect
	github.com/AmeerHussainEmumba/SmartCourse/libs/outbox v0.0.0 // indirect
	github.com/AmeerHussainEmumba/SmartCourse/libs/ratelimit v0.0.0 // indirect
	github.com/AmeerHussainEmumba/SmartCourse/libs/relay v0.0.0 // indirect
	github.com/bytedance/gopkg v0.1.3 // indirect
	github.com/bytedance/sonic v1.15.0 // indirect
	github.com/bytedance/sonic/loader v0.5.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/cloudwego/base64x v0.1.6 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/facebookgo/clock v0.0.0-20150410010913-600d898af40a // indirect
	github.com/gabriel-vasile/mimetype v1.4.12 // indirect
	github.com/gin-contrib/sse v1.1.0 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.30.1 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/goccy/go-yaml v1.19.2 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware/v2 v2.3.2 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/nexus-rpc/nexus-proto-annotations v0.1.0 // indirect
	github.com/nexus-rpc/sdk-go v0.7.0 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/quic-go v0.59.1 // indirect
	github.com/robfig/cron v1.2.0 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	github.com/stretchr/testify v1.11.1 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.3.1 // indirect
	go.mongodb.org/mongo-driver/v2 v2.10.0 // indirect
	go.temporal.io/api v1.63.5 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/arch v0.22.0 // indirect
	golang.org/x/net v0.60.0 // indirect
	golang.org/x/sync v0.23.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
	golang.org/x/text v0.42.0 // indirect
	golang.org/x/time v0.15.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260630182238-925bb5da69e7 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260630182238-925bb5da69e7 // indirect
	google.golang.org/grpc v1.83.2 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

require (
	github.com/AmeerHussainEmumba/SmartCourse/libs/cache v0.0.0
	github.com/AmeerHussainEmumba/SmartCourse/libs/config v0.0.0
	github.com/AmeerHussainEmumba/SmartCourse/libs/db v0.0.0
	github.com/AmeerHussainEmumba/SmartCourse/libs/errors v0.0.0
	github.com/AmeerHussainEmumba/SmartCourse/libs/logging v0.0.0
	github.com/AmeerHussainEmumba/SmartCourse/libs/temporal v0.0.0
	github.com/AmeerHussainEmumba/SmartCourse/services/course v0.0.0
	github.com/AmeerHussainEmumba/SmartCourse/services/identity v0.0.0
)

replace (
	github.com/AmeerHussainEmumba/SmartCourse/libs/auth => ../../libs/auth
	github.com/AmeerHussainEmumba/SmartCourse/libs/cache => ../../libs/cache
	github.com/AmeerHussainEmumba/SmartCourse/libs/config => ../../libs/config
	github.com/AmeerHussainEmumba/SmartCourse/libs/db => ../../libs/db
	github.com/AmeerHussainEmumba/SmartCourse/libs/errors => ../../libs/errors
	github.com/AmeerHussainEmumba/SmartCourse/libs/httpx => ../../libs/httpx
	github.com/AmeerHussainEmumba/SmartCourse/libs/idempotency => ../../libs/idempotency
	github.com/AmeerHussainEmumba/SmartCourse/libs/infra => ../../libs/infra
	github.com/AmeerHussainEmumba/SmartCourse/libs/logging => ../../libs/logging
	github.com/AmeerHussainEmumba/SmartCourse/libs/outbox => ../../libs/outbox
	github.com/AmeerHussainEmumba/SmartCourse/libs/ratelimit => ../../libs/ratelimit
	github.com/AmeerHussainEmumba/SmartCourse/libs/relay => ../../libs/relay
	github.com/AmeerHussainEmumba/SmartCourse/libs/router => ../../libs/router
	github.com/AmeerHussainEmumba/SmartCourse/libs/temporal => ../../libs/temporal
	github.com/AmeerHussainEmumba/SmartCourse/libs/testenv => ../../libs/testenv
	github.com/AmeerHussainEmumba/SmartCourse/services/course => ../../services/course
	github.com/AmeerHussainEmumba/SmartCourse/services/identity => ../../services/identity
)
