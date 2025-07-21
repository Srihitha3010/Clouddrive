package api

import (
	"clouddrive/cassandra"
	"clouddrive/s3helper"
	"context"
	"io"
    "bytes"
	"mime/multipart"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
)

func UploadHandler(session *gocql.Session) gin.HandlerFunc {
	return func(c *gin.Context) {
		file, header, err := c.Request.FormFile("file")
		if err != nil {
			c.String(http.StatusBadRequest, "Failed to read file")
			return
		}
		defer file.Close()

		fileID := gocql.TimeUUID()
		chunkKeys, err := uploadFileChunks(context.TODO(), file, fileID.String())
		if err != nil {
			c.String(http.StatusInternalServerError, "Failed to upload chunks")
			return
		}

		err = cassandra.SaveMetadata(session, fileID, header.Filename, chunkKeys)
		if err != nil {
			c.String(http.StatusInternalServerError, "Failed to save metadata")
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"file_id": fileID.String(),
			"message": "Upload successful",
		})
	}
}

func uploadFileChunks(ctx context.Context, file multipart.File, fileID string) ([]string, error) {
	var chunkKeys []string
	buffer := make([]byte, 5*1024*1024) // 5MB chunks
	index := 0

	for {
		n, err := file.Read(buffer)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n == 0 {
			break
		}

		chunkKey := fileID + "_chunk_" + string(index)
		err = s3helper.UploadToS3(ctx, bytes.NewReader(buffer[:n]), chunkKey)
		if err != nil {
			return nil, err
		}
		chunkKeys = append(chunkKeys, chunkKey)
		index++
	}

	return chunkKeys, nil
}

