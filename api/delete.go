package api

import (
	"clouddrive/cassandra"
	"clouddrive/s3helper"
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
)

func DeleteFileHandler(session *gocql.Session) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		fileUUID, err := gocql.ParseUUID(id)
		if err != nil {
			c.String(http.StatusBadRequest, "Invalid UUID")
			return
		}

		_, chunkKeys, err := cassandra.GetMetadata(session, fileUUID)
		if err != nil {
			c.String(http.StatusNotFound, "Metadata not found")
			return
		}

		for _, key := range chunkKeys {
			if err := s3helper.DeleteChunksFromS3(context.TODO(), []string{key}); err != nil {
				c.String(http.StatusInternalServerError, "Failed to delete chunk: "+key)
				return
			}
		}

		err = cassandra.DeleteMetadata(session, fileUUID)
		if err != nil {
			c.String(http.StatusInternalServerError, "Failed to delete metadata")
			return
		}

		c.String(http.StatusOK, "File deleted successfully")
	}
}
