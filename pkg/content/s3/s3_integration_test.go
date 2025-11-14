//go:build integration
// +build integration

package s3

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/marmos91/dittofs/pkg/content/testing"
)

// TestS3ContentStore_Integration runs the complete content store test suite
// against a real S3-compatible service (Localstack).
//
// Prerequisites:
//   - Localstack running on localhost:4566
//   - Run with: go test -tags=integration ./pkg/content/s3/...
//
// To start Localstack:
//
//	docker run --rm -p 4566:4566 localstack/localstack
func TestS3ContentStore_Integration(t *testing.T) {
	ctx := context.Background()

	// ========================================================================
	// Setup: Create S3 client connected to Localstack
	// ========================================================================

	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}

	cfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithRegion("us-east-1"),
		awsConfig.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               endpoint,
					HostnameImmutable: true,
					Source:            aws.EndpointSourceCustom,
				}, nil
			},
		)),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"test", // AccessKeyID
			"test", // SecretAccessKey
			"",     // SessionToken
		)),
	)
	if err != nil {
		t.Fatalf("Failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true // Required for Localstack
	})

	// ========================================================================
	// Create test bucket
	// ========================================================================

	bucketName := "dittofs-test-bucket"

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	// Cleanup bucket after test
	defer func() {
		// List and delete all objects first
		listResp, _ := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if listResp != nil {
			for _, obj := range listResp.Contents {
				client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    obj.Key,
				})
			}
		}

		// Delete bucket
		client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
	}()

	// ========================================================================
	// Create S3 content store
	// ========================================================================

	store, err := NewS3ContentStore(ctx, S3ContentStoreConfig{
		Client:    client,
		Bucket:    bucketName,
		KeyPrefix: "test/",
		PartSize:  5 * 1024 * 1024, // 5MB parts
	})
	if err != nil {
		t.Fatalf("Failed to create S3 content store: %v", err)
	}

	// ========================================================================
	// Run standard test suite
	// ========================================================================

	t.Run("BasicOperations", func(t *testing.T) {
		testing.RunBasicTests(t, store)
	})

	t.Run("WriteOperations", func(t *testing.T) {
		testing.RunWriteTests(t, store)
	})

	t.Run("GarbageCollection", func(t *testing.T) {
		testing.RunGCTests(t, store)
	})

	t.Run("StorageStats", func(t *testing.T) {
		testing.RunStatsTests(t, store)
	})
}

// TestS3ContentStore_Multipart tests multipart upload functionality.
func TestS3ContentStore_Multipart(t *testing.T) {
	ctx := context.Background()

	// ========================================================================
	// Setup: Create S3 client connected to Localstack
	// ========================================================================

	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}

	cfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithRegion("us-east-1"),
		awsConfig.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               endpoint,
					HostnameImmutable: true,
					Source:            aws.EndpointSourceCustom,
				}, nil
			},
		)),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"test",
			"test",
			"",
		)),
	)
	if err != nil {
		t.Fatalf("Failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	// ========================================================================
	// Create test bucket
	// ========================================================================

	bucketName := "dittofs-multipart-test"

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	defer func() {
		listResp, _ := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if listResp != nil {
			for _, obj := range listResp.Contents {
				client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    obj.Key,
				})
			}
		}
		client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
	}()

	// ========================================================================
	// Create S3 content store
	// ========================================================================

	store, err := NewS3ContentStore(ctx, S3ContentStoreConfig{
		Client:   client,
		Bucket:   bucketName,
		PartSize: 5 * 1024 * 1024,
	})
	if err != nil {
		t.Fatalf("Failed to create S3 content store: %v", err)
	}

	// ========================================================================
	// Test multipart upload
	// ========================================================================

	t.Run("MultipartUpload", func(t *testing.T) {
		contentID := "multipart-test-content"

		// Begin multipart upload
		uploadID, err := store.BeginMultipartUpload(ctx, contentID)
		if err != nil {
			t.Fatalf("Failed to begin multipart upload: %v", err)
		}

		// Upload 3 parts (each 5MB)
		partSize := 5 * 1024 * 1024
		for i := 1; i <= 3; i++ {
			data := make([]byte, partSize)
			for j := range data {
				data[j] = byte(i) // Fill with part number
			}

			err = store.UploadPart(ctx, contentID, uploadID, i, data)
			if err != nil {
				t.Fatalf("Failed to upload part %d: %v", i, err)
			}
		}

		// Complete multipart upload
		err = store.CompleteMultipartUpload(ctx, contentID, uploadID, []int{1, 2, 3})
		if err != nil {
			t.Fatalf("Failed to complete multipart upload: %v", err)
		}

		// Verify content size
		size, err := store.GetContentSize(ctx, contentID)
		if err != nil {
			t.Fatalf("Failed to get content size: %v", err)
		}

		expectedSize := uint64(3 * partSize)
		if size != expectedSize {
			t.Errorf("Expected size %d, got %d", expectedSize, size)
		}

		// Verify content exists
		exists, err := store.ContentExists(ctx, contentID)
		if err != nil {
			t.Fatalf("Failed to check content exists: %v", err)
		}
		if !exists {
			t.Error("Content should exist after multipart upload")
		}
	})

	t.Run("AbortMultipartUpload", func(t *testing.T) {
		contentID := "abort-test-content"

		// Begin multipart upload
		uploadID, err := store.BeginMultipartUpload(ctx, contentID)
		if err != nil {
			t.Fatalf("Failed to begin multipart upload: %v", err)
		}

		// Upload one part
		data := make([]byte, 5*1024*1024)
		err = store.UploadPart(ctx, contentID, uploadID, 1, data)
		if err != nil {
			t.Fatalf("Failed to upload part: %v", err)
		}

		// Abort multipart upload
		err = store.AbortMultipartUpload(ctx, contentID, uploadID)
		if err != nil {
			t.Fatalf("Failed to abort multipart upload: %v", err)
		}

		// Verify content doesn't exist
		exists, err := store.ContentExists(ctx, contentID)
		if err != nil {
			t.Fatalf("Failed to check content exists: %v", err)
		}
		if exists {
			t.Error("Content should not exist after abort")
		}
	})
}

// TestS3ContentStore_StreamingOperations tests streaming read/write.
func TestS3ContentStore_StreamingOperations(t *testing.T) {
	ctx := context.Background()

	endpoint := os.Getenv("LOCALSTACK_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4566"
	}

	cfg, err := awsConfig.LoadDefaultConfig(ctx,
		awsConfig.WithRegion("us-east-1"),
		awsConfig.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:               endpoint,
					HostnameImmutable: true,
					Source:            aws.EndpointSourceCustom,
				}, nil
			},
		)),
		awsConfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			"test",
			"test",
			"",
		)),
	)
	if err != nil {
		t.Fatalf("Failed to load AWS config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
	})

	bucketName := "dittofs-streaming-test"

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	defer func() {
		listResp, _ := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(bucketName),
		})
		if listResp != nil {
			for _, obj := range listResp.Contents {
				client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucketName),
					Key:    obj.Key,
				})
			}
		}
		client.DeleteBucket(ctx, &s3.DeleteBucketInput{
			Bucket: aws.String(bucketName),
		})
	}()

	store, err := NewS3ContentStore(ctx, S3ContentStoreConfig{
		Client: client,
		Bucket: bucketName,
	})
	if err != nil {
		t.Fatalf("Failed to create S3 content store: %v", err)
	}

	// Run streaming tests
	testing.RunStreamingTests(t, store)
}
