package harness

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/google/uuid"
)

// NewStudents registers and logs in n fresh students with up to `workers`
// requests in flight. It is NewStudent for a load scenario: the goroutines
// return their errors, and only the calling (test) goroutine fails the test.
func (s *Stack) NewStudents(t testing.TB, n, workers int) []*User {
	t.Helper()
	users := make([]*User, n)
	errs := make([]error, n)
	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				users[i], errs[i] = s.newStudent(fmt.Sprintf("Load%03d", i))
			}
		}()
	}
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("student %d: %v", i, err)
		}
	}
	return users
}

func (s *Stack) newStudent(name string) (*User, error) {
	email := uniqueEmail("student")
	code, raw, err := Do("POST", s.IdentityURL+"/api/v1/auth/register", "", map[string]string{
		"email": email, "password": Password, "full_name": name + " E2E",
	})
	if err != nil || code != http.StatusAccepted {
		return nil, fmt.Errorf("register %s: HTTP %d %s (err %v)", email, code, raw, err)
	}
	code, raw, err = Do("POST", s.IdentityURL+"/api/v1/auth/login", "", map[string]string{"email": email, "password": Password})
	if err != nil || code != http.StatusOK {
		return nil, fmt.Errorf("login %s: HTTP %d %s (err %v)", email, code, raw, err)
	}
	var r struct {
		AccessToken string `json:"access_token"`
		User        struct {
			ID       string `json:"id"`
			FullName string `json:"full_name"`
			Role     string `json:"role"`
		} `json:"user"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("decode login %s: %w", raw, err)
	}
	id, err := uuid.Parse(r.User.ID)
	if err != nil || r.AccessToken == "" {
		return nil, fmt.Errorf("login %s: no user id or token in %s", email, raw)
	}
	return &User{Email: email, ID: id, Name: r.User.FullName, Role: r.User.Role, Token: r.AccessToken}, nil
}
