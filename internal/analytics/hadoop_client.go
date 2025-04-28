package analytics

import (
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/ShvetsovYura/pkafka_final/internal/types"
	"github.com/colinmarc/hdfs/v2"
)

type HadoopClient struct {
	hc      *hdfs.Client
	dataDir string
}

func NewHadoopClient(config types.HadoopClientConfig) (*HadoopClient, error) {
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
	return &HadoopClient{
		hc:      hdfsClient,
		dataDir: "/data",
	}, nil

}

func (h *HadoopClient) MakeDirs() error {
	if err := h.hc.MkdirAll(h.dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %w", err)
	}
	return nil
}

func (h *HadoopClient) Write(userID string, value string) error {
	// Retry logic for HDFS write
	var writer *hdfs.FileWriter

	hdfsFile := fmt.Sprintf("/data/message_%s", userID)

	writer, err := h.hc.Create(hdfsFile)
	if err != nil {
		return fmt.Errorf("failed to create HDFS file: %w", err)
	}
	defer func() {
		writer.Close()
	}()

	_, err = writer.Write([]byte(value + "\n"))
	if err != nil {
		return fmt.Errorf("failed to write to HDFS file: %w", err)
	}

	err = writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close HDFS file: %w", err)
	}

	slog.Info("Message written to HDFS", slog.String("content", value), slog.String("path", hdfsFile))
	return nil
}
