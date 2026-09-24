-- Identity (RFC §5.3.1). UUIDv7 primary keys are generated application-side
-- (ADR-0005) — no DB-side default here by design.

CREATE TYPE user_role AS ENUM ('student', 'instructor', 'admin');

CREATE TABLE users (
    id              UUID PRIMARY KEY,
    email           CITEXT      NOT NULL UNIQUE,
    password_hash   TEXT        NOT NULL,
    full_name       TEXT        NOT NULL,
    role            user_role   NOT NULL DEFAULT 'student',
    status          TEXT        NOT NULL DEFAULT 'active'
                    CHECK (status IN ('active','suspended','deleted')),
    -- Backs the privilege-sensitive revocation check (ADR-0015 addendum):
    -- incremented on role/status change, embedded in the access token,
    -- checked (via Redis cache) only on admin / privilege-mutating routes.
    token_version   SMALLINT    NOT NULL DEFAULT 1,
    email_verified_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE refresh_tokens (
    id              UUID PRIMARY KEY,
    user_id         UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash      BYTEA       NOT NULL UNIQUE,
    family_id       UUID        NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    revoked_at      TIMESTAMPTZ,
    replaced_by     UUID        REFERENCES refresh_tokens(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_refresh_user_active ON refresh_tokens(user_id) WHERE revoked_at IS NULL;
CREATE INDEX idx_refresh_family ON refresh_tokens(family_id);

-- Password reset and email verification (differences-from-requirements.md
-- DR-008): same hashed-single-use-token shape as refresh_tokens, so a
-- database disclosure yields no usable token either way.

CREATE TABLE password_reset_tokens (
    id              UUID PRIMARY KEY,
    user_id         UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash      BYTEA       NOT NULL UNIQUE,
    expires_at      TIMESTAMPTZ NOT NULL,
    used_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_password_reset_user_active ON password_reset_tokens(user_id) WHERE used_at IS NULL;

CREATE TABLE email_verification_tokens (
    id              UUID PRIMARY KEY,
    user_id         UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash      BYTEA       NOT NULL UNIQUE,
    expires_at      TIMESTAMPTZ NOT NULL,
    used_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_email_verify_user_active ON email_verification_tokens(user_id) WHERE used_at IS NULL;
