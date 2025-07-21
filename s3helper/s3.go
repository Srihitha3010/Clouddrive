
package s3helper

import (
    "bytes"
    "context"
    "fmt"
    "io"
    "log"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/s3"
    "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var (
    client     *s3.Client
    bucketName = "clouddrive-files"
)

// Initialize AWS S3 client
func init() {
    cfg, err := config.LoadDefaultConfig(context.TODO())
    if err != nil {
        log.Fatalf("unable to load AWS SDK config, %v", err)
    }
    client = s3.NewFromConfig(cfg)
}

// UploadToS3 uploads a chunk to S3 using the provided reader and chunk key
func UploadToS3(ctx context.Context, reader io.Reader, key string) error {
    _, err := client.PutObject(ctx, &s3.PutObjectInput{
        Bucket: aws.String(bucketName),
        Key:    aws.String(key),
        Body:   reader,
        ACL:    types.ObjectCannedACLPrivate,
    })
    if err != nil {
        return fmt.Errorf("failed to upload chunk %s: %w", key, err)
    }
    return nil
}

// RetrieveFileChunks downloads multiple chunks from S3 and concatenates them
func RetrieveFileChunks(ctx context.Context, chunkKeys []string) ([]byte, error) {
    var fullFile bytes.Buffer

    for _, key := range chunkKeys {
        output, err := client.GetObject(ctx, &s3.GetObjectInput{
            Bucket: aws.String(bucketName),
            Key:    aws.String(key),
        })
        if err != nil {
            return nil, fmt.Errorf("failed to download chunk %s: %w", key, err)
        }

        _, err = io.Copy(&fullFile, output.Body)
        output.Body.Close()
        if err != nil {
            return nil, fmt.Errorf("failed to read chunk %s: %w", key, err)
        }
    }

    return fullFile.Bytes(), nil
}

// DeleteChunksFromS3 removes chunks from S3 by their keys
func DeleteChunksFromS3(ctx context.Context, chunkKeys []string) error {
    for _, key := range chunkKeys {
        _, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
            Bucket: aws.String(bucketName),
            Key:    aws.String(key),
        })
        if err != nil {
            return fmt.Errorf("failed to delete chunk %s: %w", key, err)
        }
    }
    return nil
}



