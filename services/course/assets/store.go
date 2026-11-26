// Package assets implements course.AssetStore on an S3-compatible object store
// (MinIO locally, ADR-0019) using the AWS SDK for Go v2, the client ADR-0019
// names: the application is coded against the S3 API, so moving to real S3 is
// a configuration change. It imports services/course/course for the interface
// types, never the other way, the same direction services/course/activities uses.
package assets

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/AmeerHussainEmumba/SmartCourse/libs/config"
	"github.com/AmeerHussainEmumba/SmartCourse/services/course/course"
)

// region is a placeholder: MinIO ignores it, but SigV4 needs one, and the same
// value must be used for signing and verifying.
const region = "us-east-1"

// Store is an S3-backed course.AssetStore.
type Store struct {
	bucket  string
	client  *s3.Client        // dials the internal endpoint: Stat
	presign *s3.PresignClient // signs for the public endpoint: PresignUpload
}

var _ course.AssetStore = (*Store)(nil)

// New builds a Store from the MinIO settings. Pre-signed URLs use
// cfg.PublicEndpoint when set (what a browser can reach), otherwise
// cfg.Endpoint. The bucket must already exist (Compose's minio-init creates it).
func New(cfg config.MinIOConfig) (*Store, error) {
	if cfg.BucketAssets == "" {
		return nil, errors.New("assets: bucket name is empty")
	}
	internal, err := endpointURL(cfg.Endpoint, cfg.UseSSL)
	if err != nil {
		return nil, err
	}
	public := internal
	if cfg.PublicEndpoint != "" {
		if public, err = endpointURL(cfg.PublicEndpoint, cfg.UseSSL); err != nil {
			return nil, err
		}
	}
	return &Store{
		bucket:  cfg.BucketAssets,
		client:  newClient(internal, cfg),
		presign: s3.NewPresignClient(newClient(public, cfg)),
	}, nil
}

func newClient(endpoint string, cfg config.MinIOConfig) *s3.Client {
	return s3.New(s3.Options{
		Region:       region,
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		UsePathStyle: true, // MinIO serves buckets as path prefixes, not subdomains
		// Newer SDK versions add a CRC checksum header to every PutObject by
		// default; a browser PUT of the raw file would not send it and the
		// signature would not match. Only compute checksums when required.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
}

func endpointURL(endpoint string, useSSL bool) (string, error) {
	endpoint = strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if endpoint == "" {
		return "", errors.New("assets: MinIO endpoint is empty")
	}
	if strings.Contains(endpoint, "://") {
		return endpoint, nil
	}
	if useSSL {
		return "https://" + endpoint, nil
	}
	return "http://" + endpoint, nil
}

// PresignUpload returns a PUT URL that storage will only accept with exactly
// this content type and this content length: both are part of the signature,
// so a client cannot upload a different type or size under the issued key.
func (s *Store) PresignUpload(ctx context.Context, key, contentType string, size int64, expires time.Duration) (course.PresignedUpload, error) {
	req, err := s.presign.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		ContentType:   aws.String(contentType),
		ContentLength: aws.Int64(size),
	}, s3.WithPresignExpires(expires))
	if err != nil {
		return course.PresignedUpload{}, err
	}
	return course.PresignedUpload{
		URL:       req.URL,
		Headers:   map[string]string{"Content-Type": contentType},
		ExpiresAt: time.Now().Add(expires).UTC(),
	}, nil
}

// Stat does a HeadObject: existence, size and content type, never the bytes.
func (s *Store) Stat(ctx context.Context, key string) (course.AssetInfo, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		var nf *types.NotFound
		var apiErr smithy.APIError
		if errors.As(err, &nf) || (errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NotFound" || apiErr.ErrorCode() == "NoSuchKey")) {
			return course.AssetInfo{}, course.ErrAssetNotFound
		}
		return course.AssetInfo{}, fmt.Errorf("head object: %w", err)
	}
	return course.AssetInfo{Size: aws.ToInt64(out.ContentLength), ContentType: aws.ToString(out.ContentType)}, nil
}
