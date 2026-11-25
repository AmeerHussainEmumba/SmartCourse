// Command seed populates a running SmartCourse stack with a realistic dataset
// (instructors, courses at every lifecycle state, students with partial
// progress) so a contributor gets a working system with data in one command
// (`make seed`).
//
// It is a plain HTTP client of the public APIs (identity-api, course-api;
// analytics-api is only health-checked) and requires no service module, so it
// also exercises the real contracts: registration, the admin role endpoint,
// course/module/lesson creation, the real publish workflow, enrollment and
// lesson progress. The one account the API cannot create, the first admin, is
// created beforehand by the identity-admin CLI (the Makefile target does).
//
// Idempotent: every account is found by logging in, every course by its slug,
// every enrollment by listing the student's own, so a second run creates
// nothing new and a partial run is topped up.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"time"
)

type config struct {
	IdentityURL, CourseURL, AnalyticsURL string
	AdminEmail, AdminPassword            string
	// Allow lets the tool run against a non-loopback target (SEED_ALLOW=1).
	Allow bool
	// SkipPublish skips everything that needs the publish workflow
	// (SEED_SKIP_PUBLISH=1), for a stack without Temporal.
	SkipPublish       bool
	ReadyWait         time.Duration
	PublishTimeout    time.Duration
	PublishPoll       time.Duration
	PublishStartTries int
	PublishRetryStep  time.Duration
}

// loadConfig reads the environment. The admin defaults match env.example's
// documented first-admin values.
func loadConfig(get func(string) string) config {
	or := func(key, def string) string {
		if v := get(key); v != "" {
			return v
		}
		return def
	}
	return config{
		IdentityURL:       trimSlash(or("IDENTITY_URL", "http://localhost:8090")),
		CourseURL:         trimSlash(or("COURSE_URL", "http://localhost:8091")),
		AnalyticsURL:      trimSlash(or("ANALYTICS_URL", "http://localhost:8092")),
		AdminEmail:        or("SEED_ADMIN_EMAIL", "admin@smartcourse.local"),
		AdminPassword:     or("SEED_ADMIN_PASSWORD", "change-me-admin-password"),
		Allow:             get("SEED_ALLOW") == "1",
		SkipPublish:       get("SEED_SKIP_PUBLISH") == "1",
		ReadyWait:         seconds(get("SEED_READY_WAIT_S"), 120),
		PublishTimeout:    seconds(get("SEED_PUBLISH_TIMEOUT_S"), 60),
		PublishPoll:       500 * time.Millisecond,
		PublishStartTries: 5,
		PublishRetryStep:  time.Second,
	}
}

func trimSlash(s string) string {
	for len(s) > 0 && s[len(s)-1] == '/' {
		s = s[:len(s)-1]
	}
	return s
}

func seconds(v string, def int) time.Duration {
	if n, err := strconv.Atoi(v); err == nil && n > 0 {
		return time.Duration(n) * time.Second
	}
	return time.Duration(def) * time.Second
}

// checkTargets is the guard against pointing the seed at something real: it
// creates accounts with a publicly documented password, so every target URL
// must be loopback unless SEED_ALLOW=1.
func checkTargets(cfg config) error {
	for _, raw := range []string{cfg.IdentityURL, cfg.CourseURL, cfg.AnalyticsURL} {
		u, err := url.Parse(raw)
		if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return fmt.Errorf("%q is not a valid http(s) URL", raw)
		}
		if !cfg.Allow && !isLoopback(u.Hostname()) {
			return fmt.Errorf("refusing to seed %s: it is not a loopback address and this tool creates accounts with a known password; "+
				"set SEED_ALLOW=1 only if the target is a disposable environment", u.Hostname())
		}
	}
	return nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(os.Getenv)

	// The accounts it creates share a publicly documented password, so it
	// refuses to run in any environment but development (APP_ENV is unset in
	// the dev shell) and against anything but a loopback target.
	if env := os.Getenv("APP_ENV"); env != "" && env != "development" {
		logger.Error("refusing to seed: this tool creates accounts with a known password and only runs when APP_ENV=development",
			slog.String("app_env", env))
		os.Exit(1)
	}
	if err := checkTargets(cfg); err != nil {
		logger.Error("refusing to seed", slog.Any("error", err))
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	code := newSeeder(cfg, logger, os.Stdout).run(ctx)
	stop()
	os.Exit(code)
}
