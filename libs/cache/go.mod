module github.com/AmeerHussainEmumba/SmartCourse/libs/cache

go 1.26.5

require github.com/redis/go-redis/v9 v9.22.0

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)

require github.com/AmeerHussainEmumba/SmartCourse/libs/config v0.0.0

replace github.com/AmeerHussainEmumba/SmartCourse/libs/config => ../config
