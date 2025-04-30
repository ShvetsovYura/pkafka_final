package analytics

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"github.com/colinmarc/hdfs/v2"
	"github.com/google/uuid"
)

type HDFSClient struct {
	hc      *hdfs.Client
	dataDir string
}

func NewHDFSClient(config types.HDFSClientConfig) (*HDFSClient, error) {
	// Initialize HDFS client with custom dial function
	dialFunc := (&net.Dialer{
		Timeout:   60 * time.Second,
		KeepAlive: 60 * time.Second,
	}).DialContext

	clientOptions := hdfs.ClientOptions{
		Addresses:           config.Addresses,
		User:                config.User,
		NamenodeDialFunc:    dialFunc,
		DatanodeDialFunc:    dialFunc,
		UseDatanodeHostname: config.UseDatanodeHostname,
	}

	hdfsClient, err := hdfs.NewClient(clientOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create HDFS client: %w", err)
	}
	return &HDFSClient{
		hc:      hdfsClient,
		dataDir: "/data",
	}, nil

}

func (h *HDFSClient) MakeDirs(path string) error {
	if err := h.hc.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("failed to create directory %w", err)
	}
	return nil
}

func (h *HDFSClient) Write(userID string, value string) error {
	// Retry logic for HDFS write
	var writer *hdfs.FileWriter
	// err := h.MakeDirs(fmt.Sprintf("/data/%s", userID))
	// if err != nil {
	// 	return err
	// }
	h.MakeDirs(fmt.Sprintf("%s/%s", h.dataDir, userID))

	// hdfsFile := fmt.Sprintf("/data/%s/%s", userID, uuid.New())
	// hdfsFile := fmt.Sprintf("/data/%s", uuid.New())
	hdfsFile := fmt.Sprintf("%s/%s/%s", h.dataDir, userID, uuid.New())
	writer, err := h.hc.Create(hdfsFile)
	if err != nil {
		return fmt.Errorf("failed to create HDFS file: %w", err)
	}
	defer func() {
		err = writer.Close()
		if err != nil {
			slog.Error("failed to close HDFS file", slog.Any("error", err))
		}
	}()

	_, err = writer.Write([]byte(value + "\n"))
	if err != nil {
		return fmt.Errorf("failed to write to HDFS file: %w", err)
	}

	slog.Info("Message written to HDFS", slog.String("content", value), slog.String("path", hdfsFile))
	return nil
}
