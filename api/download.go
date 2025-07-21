package api

import (
	"clouddrive/cassandra"
	"clouddrive/s3helper"
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
)

func DownloadHandler(session *gocql.Session) gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.Param("id")
		fileUUID, err := gocql.ParseUUID(id)
		if err != nil {
			c.String(http.StatusBadRequest, "Invalid UUID")
			return
		}

		fileName, chunkKeys, err := cassandra.GetMetadata(session, fileUUID)
		if err != nil {
			c.String(http.StatusNotFound, "Metadata not found")
			return
		}

		data, err := s3helper.RetrieveFileChunks(context.TODO(), chunkKeys)
		if err != nil {
			c.String(http.StatusInternalServerError, "Failed to retrieve file")
			return
		}

		c.Header("Content-Disposition", "attachment; filename="+fileName)
		c.Data(http.StatusOK, "application/octet-stream", data)
	}
}
