package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/AmeerHussainEmumba/SmartCourse/services/identity/identity"
)

type fakeBootstrapper struct {
	res    *identity.BootstrapAdminResult
	err    error
	called int
	gotPW  string
}

func (f *fakeBootstrapper) BootstrapAdmin(_ context.Context, _, password, _ string) (*identity.BootstrapAdminResult, error) {
	f.called++
	f.gotPW = password
	return f.res, f.err
}

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func opener(f *fakeBootstrapper, err error) func(context.Context) (bootstrapper, func(), error) {
	return func(context.Context) (bootstrapper, func(), error) { return f, func() {}, err }
}

const secretPW = "s3cret-passphrase-do-not-print"

func TestRun_MissingCredentialsIsAUsageError(t *testing.T) {
	for name, m := range map[string]map[string]string{
		"no email":    {"IDENTITY_ADMIN_PASSWORD": secretPW},
		"no password": {"IDENTITY_ADMIN_EMAIL": "a@example.com"},
		"blank email": {"IDENTITY_ADMIN_EMAIL": "  ", "IDENTITY_ADMIN_PASSWORD": secretPW},
	} {
		f := &fakeBootstrapper{}
		var out, errb bytes.Buffer
		if code := run(context.Background(), env(m), &out, &errb, opener(f, nil)); code != 2 || f.called != 0 {
			t.Errorf("%s: exit %d, called %d; want 2 and no call", name, code, f.called)
		}
		if strings.Contains(out.String()+errb.String(), secretPW) {
			t.Errorf("%s: password printed", name)
		}
	}
}

func TestRun_CreatedAndExistingBothExitZeroAndNeverPrintTheSecret(t *testing.T) {
	id := uuid.New()
	m := map[string]string{"IDENTITY_ADMIN_EMAIL": "root@example.com", "IDENTITY_ADMIN_PASSWORD": secretPW}
	for name, tc := range map[string]struct {
		res  *identity.BootstrapAdminResult
		want string
	}{
		"created": {&identity.BootstrapAdminResult{UserID: id, Created: true, Role: identity.RoleAdmin}, "created admin"},
		"exists":  {&identity.BootstrapAdminResult{UserID: id, Role: identity.RoleAdmin}, "already exists"},
	} {
		f := &fakeBootstrapper{res: tc.res}
		var out, errb bytes.Buffer
		if code := run(context.Background(), env(m), &out, &errb, opener(f, nil)); code != 0 {
			t.Errorf("%s: exit %d (%s)", name, code, errb.String())
		}
		if !strings.Contains(out.String(), tc.want) {
			t.Errorf("%s: output %q lacks %q", name, out.String(), tc.want)
		}
		if strings.Contains(out.String()+errb.String(), secretPW) {
			t.Errorf("%s: password printed", name)
		}
		if f.gotPW != secretPW {
			t.Errorf("%s: the password did not reach the service unchanged", name)
		}
	}
}

func TestRun_FailuresExitNonZero(t *testing.T) {
	m := map[string]string{"IDENTITY_ADMIN_EMAIL": "root@example.com", "IDENTITY_ADMIN_PASSWORD": secretPW}
	var out, errb bytes.Buffer
	if code := run(context.Background(), env(m), &out, &errb, opener(nil, errors.New("db down"))); code != 1 {
		t.Errorf("connect failure: exit %d, want 1", code)
	}
	f := &fakeBootstrapper{err: errors.New("invalid admin credentials: Password: min")}
	if code := run(context.Background(), env(m), &out, &errb, opener(f, nil)); code != 2 {
		t.Errorf("validation failure: exit %d, want 2", code)
	}
	f = &fakeBootstrapper{err: errors.New("boom")}
	if code := run(context.Background(), env(m), &out, &errb, opener(f, nil)); code != 1 {
		t.Errorf("service failure: exit %d, want 1", code)
	}
}

func TestRun_DefaultsTheName(t *testing.T) {
	var gotName string
	f := &nameSpy{name: &gotName}
	m := map[string]string{"IDENTITY_ADMIN_EMAIL": "root@example.com", "IDENTITY_ADMIN_PASSWORD": secretPW}
	var out, errb bytes.Buffer
	run(context.Background(), env(m), &out, &errb, func(context.Context) (bootstrapper, func(), error) { return f, func() {}, nil })
	if gotName != defaultName {
		t.Errorf("name = %q, want %q", gotName, defaultName)
	}
}

type nameSpy struct{ name *string }

func (n *nameSpy) BootstrapAdmin(_ context.Context, _, _, name string) (*identity.BootstrapAdminResult, error) {
	*n.name = name
	return &identity.BootstrapAdminResult{UserID: uuid.New(), Created: true, Role: identity.RoleAdmin}, nil
}
