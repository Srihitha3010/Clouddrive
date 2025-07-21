package main

import (
    "clouddrive/api"
    "log"
    "clouddrive/cassandra"
    "github.com/gin-gonic/gin"
)

func main() {
    session, err := cassandra.CreateSession()
if err != nil {
    log.Fatalf("Failed to connect to Cassandra: %v", err)
}
defer session.Close()

    r := gin.Default()

    r.POST("/upload", api.UploadHandler(session))
    r.GET("/download/:id", api.DownloadHandler(session))
    r.DELETE("/delete/:id", api.DeleteFileHandler(session))

    r.Run(":8080")
}
