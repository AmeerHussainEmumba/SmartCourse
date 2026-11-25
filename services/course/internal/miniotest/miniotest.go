// Package miniotest starts a real MinIO container for integration tests (RFC
// §18.2: no mocks for the storage whose behaviour is being verified: signature
// checks, content-length enforcement and HeadObject results are exactly what
// the asset tests need to see). It lives in services/course rather than
// libs/testenv because only this module needs object storage today. Requires
// Docker. It uses the same image docker-compose.yml runs.
package miniotest

import (
	"bytes"
	"context"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
)

const (
	image  = "minio/minio:RELEASE.2025-09-07T16-13-09Z@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e" // S-13: same pin as docker-compose.yml
	user   = "smartcourse-test"
	secret = "smartcourse-test-secret"
	Bucket = "smartcourse-assets-test"
)

// Env is a running MinIO with its bucket created.
type Env struct {
	Config config.MinIOConfig
	client *s3.Client
}

// Setup starts MinIO, creates Bucket, and tears the container down with the test.
func Setup(t *testing.T) *Env {
	t.Helper()
	ctx := context.Background()
	ctr, err := tcminio.Run(ctx, image, tcminio.WithUsername(user), tcminio.WithPassword(secret))
	if err != nil {
		t.Fatalf("start minio container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
	hostPort, err := ctr.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("minio endpoint: %v", err)
	}
	endpoint := "http://" + hostPort
	client := s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(user, secret, ""),
		UsePathStyle: true,
	})
	if _, err := client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(Bucket)}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	return &Env{
		Config: config.MinIOConfig{Endpoint: endpoint, AccessKeyID: user, SecretAccessKey: secret, BucketAssets: Bucket},
		client: client,
	}
}

// Put writes an object directly (bypassing the pre-signed URL path), for
// arranging "present but wrong" objects.
func (e *Env) Put(t *testing.T, key, contentType string, body []byte) {
	t.Helper()
	_, err := e.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: aws.String(Bucket), Key: aws.String(key), ContentType: aws.String(contentType),
		Body: bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("put %s: %v", key, err)
	}
}

// Upload performs the PUT a browser would do with a pre-signed URL: the given
// headers verbatim and a body of the given bytes. It returns the HTTP status.
func Upload(t *testing.T, url string, headers map[string]string, body []byte) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		t.Fatalf("build upload request: %v", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode
}
