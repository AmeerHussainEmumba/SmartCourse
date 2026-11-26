package config

import (
	"fmt"
	"net/url"
	"strings"
	"time"
)

// MongoConfig locates the MongoDB that holds the event archive, the audit log
// and failed_events (libs/mongostore, ADR-0031). MongoDB is optional unless a
// binary asks for it (Requirements.Mongo makes MONGO_URI required): with URI
// empty the services that can use it run degraded and say so, because ADR-0004
// keeps MongoDB off every synchronous user path.
type MongoConfig struct {
	// URI is MONGO_URI, for example mongodb://mongodb:27017. It may carry
	// credentials: never log it. Empty means MongoDB is not configured.
	URI string
	// Database is MONGO_DATABASE (default "smartcourse").
	Database string
	// ConnectTimeout bounds opening the store (MONGO_CONNECT_TIMEOUT, default 5s).
	ConnectTimeout time.Duration
	// ServerSelectionTimeout is how long an operation waits for a usable
	// server, so an outage fails fast (MONGO_SERVER_SELECTION_TIMEOUT, default 3s).
	ServerSelectionTimeout time.Duration
	// OpTimeout bounds each operation (MONGO_OP_TIMEOUT, default 5s).
	OpTimeout time.Duration
}

// Configured reports whether a MongoDB URI was provided.
func (m MongoConfig) Configured() bool { return m.URI != "" }

const maxMongoTimeout = 2 * time.Minute

func loadMongo(need bool, getenv func(string) string, optDuration func(string, time.Duration) time.Duration) (MongoConfig, []string) {
	m := MongoConfig{
		URI:                    strings.TrimSpace(getenv("MONGO_URI")),
		Database:               strings.TrimSpace(getenv("MONGO_DATABASE")),
		ConnectTimeout:         optDuration("MONGO_CONNECT_TIMEOUT", 5*time.Second),
		ServerSelectionTimeout: optDuration("MONGO_SERVER_SELECTION_TIMEOUT", 3*time.Second),
		OpTimeout:              optDuration("MONGO_OP_TIMEOUT", 5*time.Second),
	}
	if m.Database == "" {
		m.Database = "smartcourse"
	}
	var errs []string
	if need && m.URI == "" {
		errs = append(errs, "MONGO_URI is required")
	}
	if m.URI != "" {
		// Only the scheme and shape are checked, and the URI is never echoed:
		// it may hold a password.
		u, err := url.Parse(m.URI)
		switch {
		case err != nil:
			errs = append(errs, "MONGO_URI is not a valid URI")
		case u.Scheme != "mongodb" && u.Scheme != "mongodb+srv":
			errs = append(errs, "MONGO_URI must start with mongodb:// or mongodb+srv://")
		case u.Host == "":
			errs = append(errs, "MONGO_URI must name a host")
		}
	}
	if strings.ContainsAny(m.Database, "/\\. \"$*<>:|?\x00") {
		errs = append(errs, "MONGO_DATABASE contains a character MongoDB does not allow in a database name")
	}
	if len(m.Database) > 63 {
		errs = append(errs, "MONGO_DATABASE must be at most 63 characters")
	}
	for key, d := range map[string]time.Duration{
		"MONGO_CONNECT_TIMEOUT": m.ConnectTimeout, "MONGO_SERVER_SELECTION_TIMEOUT": m.ServerSelectionTimeout, "MONGO_OP_TIMEOUT": m.OpTimeout,
	} {
		if d <= 0 || d > maxMongoTimeout {
			errs = append(errs, fmt.Sprintf("%s must be greater than zero and at most %s", key, maxMongoTimeout))
		}
	}
	return m, errs
}
