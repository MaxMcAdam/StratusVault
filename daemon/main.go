package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/MaxMcAdam/StratusVault/client"
	"github.com/MaxMcAdam/StratusVault/client/config"
	"github.com/MaxMcAdam/StratusVault/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// WatchedFile represents a file being monitored
type WatchedFile struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	LocalPath string `json:"local_path"`
	LastToken uint64 `json:"last_token"`
	LastSize  int64  `json:"last_size"`
	Enabled   bool   `json:"enabled"`
}

// DaemonConfig holds daemon configuration
type DaemonConfig struct {
	ServerAddress   string        `json:"server_address"`
	PollInterval    time.Duration `json:"poll_interval"`
	StateFile       string        `json:"state_file"`
	LogFile         string        `json:"log_file"`
	MaxRetries      int           `json:"max_retries"`
	RetryDelay      time.Duration `json:"retry_delay"`
	ChunkSize       int64         `json:"chunk_size"`
	ConcurrentFiles int           `json:"concurrent_files"`
	WatchedFiles    []WatchedFile `json:"watched_files"`
}

// Type used to read config file
// Necessary since the json entry cannot demarshal directly into a time.duration
type daemonConfigFile struct {
	ServerAddress   string        `json:"server_address"`
	PollIntervalStr string        `json:"poll_interval"`
	StateFile       string        `json:"state_file"`
	LogFile         string        `json:"log_file"`
	MaxRetries      int           `json:"max_retries"`
	RetryDelayStr   string        `json:"retry_delay"`
	ChunkSize       int64         `json:"chunk_size"`
	ConcurrentFiles int           `json:"concurrent_files"`
	WatchedFiles    []WatchedFile `json:"watched_files"`
}

// Convert the read-in file to a DaemonConfig struct
func (f daemonConfigFile) fromFileConfig() (DaemonConfig, error) {
	config := DaemonConfig{}
	pollInt, err := time.ParseDuration(f.PollIntervalStr)
	if err != nil {
		return config, nil
	}
	retryDel, err := time.ParseDuration(f.RetryDelayStr)
	if err != nil {
		return config, nil
	}
	config = DaemonConfig{ServerAddress: f.ServerAddress,
		PollInterval:    pollInt,
		StateFile:       f.StateFile,
		LogFile:         f.LogFile,
		MaxRetries:      f.MaxRetries,
		RetryDelay:      retryDel,
		ChunkSize:       f.ChunkSize,
		ConcurrentFiles: f.ConcurrentFiles,
		WatchedFiles:    f.WatchedFiles,
	}

	return config, nil
}

// FileSyncDaemon manages file synchronization
type FileSyncDaemon struct {
	config       *DaemonConfig
	client       proto.FileServiceClient
	conn         *grpc.ClientConn
	logger       *log.Logger
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	mu           sync.RWMutex
	watchedFiles map[string]*WatchedFile
	semaphore    chan struct{}
}

// NewFileSyncDaemon creates a new daemon instance
func NewFileSyncDaemon(configPath string) (*FileSyncDaemon, error) {
	config, err := loadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	// Setup logging
	var logger *log.Logger
	if config.LogFile != "" {
		logFile, err := os.OpenFile(config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %w", err)
		}
		logger = log.New(logFile, "[StratusVault] ", log.LstdFlags|log.Lshortfile)
	} else {
		logger = log.New(os.Stdout, "[StratusVault] ", log.LstdFlags|log.Lshortfile)
	}

	ctx, cancel := context.WithCancel(context.Background())

	daemon := &FileSyncDaemon{
		config:       config,
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
		watchedFiles: make(map[string]*WatchedFile),
		semaphore:    make(chan struct{}, config.ConcurrentFiles),
	}

	// Load watched files into map
	for i := range config.WatchedFiles {
		file := &config.WatchedFiles[i]
		daemon.watchedFiles[file.ID] = file
	}

	return daemon, nil
}

// Start begins the daemon operation
func (d *FileSyncDaemon) Start() error {
	d.logger.Println("Starting StratusVault file sync daemon")

	// Connect to server
	if err := d.connectToServer(); err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	// Load state from disk
	if err := d.loadState(); err != nil {
		d.logger.Printf("Warning: failed to load state: %v", err)
	}

	// Start polling loop
	d.wg.Add(1)
	go d.pollLoop()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for shutdown signal
	<-sigChan
	d.logger.Println("Shutdown signal received, stopping daemon...")

	return d.Stop()
}

// Stop gracefully shuts down the daemon
func (d *FileSyncDaemon) Stop() error {
	d.logger.Println("Stopping daemon...")

	// Cancel context to stop all operations
	d.cancel()

	// Wait for all goroutines to finish
	d.wg.Wait()

	// Save state
	if err := d.saveState(); err != nil {
		d.logger.Printf("Error saving state: %v", err)
	}

	// Close connection
	if d.conn != nil {
		d.conn.Close()
	}

	d.logger.Println("Daemon stopped")
	return nil
}

// connectToServer establishes connection to the gRPC server
func (d *FileSyncDaemon) connectToServer() error {
	conn, err := grpc.NewClient(d.config.ServerAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	d.conn = conn
	d.client = proto.NewFileServiceClient(conn)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = d.client.ListFiles(ctx, &proto.ListFilesRequest{PageSize: 1})
	if err != nil {
		d.conn.Close()
		return fmt.Errorf("failed to test connection: %w", err)
	}

	d.logger.Println("Connected to server successfully")
	return nil
}

// pollLoop is the main polling loop
func (d *FileSyncDaemon) pollLoop() {
	defer d.wg.Done()

	ticker := time.NewTicker(d.config.PollInterval)
	defer ticker.Stop()

	// Initial sync
	d.syncAllFiles()

	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			d.syncAllFiles()
		}
	}
}

// syncAllFiles checks and syncs all watched files
func (d *FileSyncDaemon) syncAllFiles() {
	d.mu.RLock()
	files := make([]*WatchedFile, 0, len(d.watchedFiles))
	for _, file := range d.watchedFiles {
		if file.Enabled {
			files = append(files, file)
		}
	}
	d.mu.RUnlock()

	if len(files) == 0 {
		return
	}

	// Prepare tokens for batch request
	tokens := make([]*proto.FileIdToken, len(files))
	for i, file := range files {
		tokens[i] = &proto.FileIdToken{
			Id:        file.ID,
			Name:      file.Name,
			LastToken: file.LastToken,
		}
	}

	// Check for events
	ctx, cancel := context.WithTimeout(d.ctx, 30*time.Second)
	defer cancel()

	resp, err := d.client.GetFileEvents(ctx, &proto.GetFileEventsRequest{
		Tokens: tokens,
	})

	if err != nil {
		d.logger.Printf("Error checking for file events: %v", err)
		return
	}

	// Process events
	for _, event := range resp.Events {
		d.wg.Add(1)
		go d.processFileEvent(event)
	}
}

// processFileEvent handles a single file event
func (d *FileSyncDaemon) processFileEvent(event *proto.FileEvent) {
	defer d.wg.Done()

	// Rate limiting
	d.semaphore <- struct{}{}
	defer func() { <-d.semaphore }()

	d.mu.RLock()
	watchedFile, exists := d.watchedFiles[event.Id]
	d.mu.RUnlock()

	if !exists || !watchedFile.Enabled {
		return
	}

	d.logger.Printf("Processing event for file %s (ID: %s): %v",
		watchedFile.Name, event.Id, event.EventType)

	switch event.EventType {
	case proto.FileEvent_EVENT_TYPE_CREATED, proto.FileEvent_EVENT_TYPE_UPDATED:
		if err := d.downloadFile(watchedFile); err != nil {
			d.logger.Printf("Error downloading file %s: %v", watchedFile.Name, err)
			return
		}

	case proto.FileEvent_EVENT_TYPE_DELETED:
		if err := d.deleteLocalFile(watchedFile); err != nil {
			d.logger.Printf("Error deleting local file %s: %v", watchedFile.LocalPath, err)
			return
		}

	case proto.FileEvent_EVENT_TYPE_RENAMED:
		// Handle rename - might need to update local path
		d.logger.Printf("File %s was renamed, updating metadata", watchedFile.Name)
	}

	// Update last token
	d.mu.Lock()
	watchedFile.LastToken = event.NextToken
	d.mu.Unlock()
}

// downloadFile downloads a file from the server with retry logic
func (d *FileSyncDaemon) downloadFile(file *WatchedFile) error {
	clientConfig := &config.ClientConfig{
		ChunkSize: d.config.ChunkSize,
	}

	var lastErr error
	for retry := 0; retry < d.config.MaxRetries; retry++ {
		if retry > 0 {
			d.logger.Printf("Retrying download of %s (attempt %d/%d)",
				file.Name, retry+1, d.config.MaxRetries)
			time.Sleep(d.config.RetryDelay)
		}

		// Create directory if needed
		dir := filepath.Dir(file.LocalPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			lastErr = fmt.Errorf("failed to create directory: %w", err)
			continue
		}

		// Download file
		err := client.DownloadFile(d.client, file.LocalPath, file.ID, clientConfig)
		if err != nil {
			lastErr = fmt.Errorf("download failed: %w", err)
			continue
		}

		// Update file info
		if stat, err := os.Stat(file.LocalPath); err == nil {
			d.mu.Lock()
			file.LastSize = stat.Size()
			d.mu.Unlock()
		}

		d.logger.Printf("Successfully downloaded %s to %s", file.Name, file.LocalPath)
		return nil
	}

	return fmt.Errorf("download failed after %d retries: %w", d.config.MaxRetries, lastErr)
}

// deleteLocalFile removes the local file
func (d *FileSyncDaemon) deleteLocalFile(file *WatchedFile) error {
	if _, err := os.Stat(file.LocalPath); os.IsNotExist(err) {
		return nil // File doesn't exist, nothing to do
	}

	if err := os.Remove(file.LocalPath); err != nil {
		return fmt.Errorf("failed to delete local file: %w", err)
	}

	d.logger.Printf("Deleted local file: %s", file.LocalPath)
	return nil
}

// loadState loads daemon state from disk
func (d *FileSyncDaemon) loadState() error {
	if d.config.StateFile == "" {
		return nil
	}

	data, err := ioutil.ReadFile(d.config.StateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No state file exists yet
		}
		return err
	}

	var state map[string]WatchedFile
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Update watched files with saved state
	for id, savedFile := range state {
		if watchedFile, exists := d.watchedFiles[id]; exists {
			watchedFile.LastToken = savedFile.LastToken
			watchedFile.LastSize = savedFile.LastSize
		}
	}

	d.logger.Printf("Loaded state for %d files", len(state))
	return nil
}

// saveState saves daemon state to disk
func (d *FileSyncDaemon) saveState() error {
	if d.config.StateFile == "" {
		return nil
	}

	d.mu.RLock()
	state := make(map[string]WatchedFile)
	for id, file := range d.watchedFiles {
		state[id] = *file
	}
	d.mu.RUnlock()

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	// Create directory if needed
	dir := filepath.Dir(d.config.StateFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Write to temporary file first, then rename for atomicity
	tempFile := d.config.StateFile + ".tmp"
	if err := ioutil.WriteFile(tempFile, data, 0644); err != nil {
		return err
	}

	return os.Rename(tempFile, d.config.StateFile)
}

// loadConfig loads configuration from file
func loadConfig(path string) (*DaemonConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var configFile daemonConfigFile
	if err := json.Unmarshal(data, &configFile); err != nil {
		return nil, err
	}

	config, err := configFile.fromFileConfig()
	if err != nil {
		return nil, err
	}

	// Set defaults
	if config.ServerAddress == "" {
		config.ServerAddress = "localhost:50051"
	}
	if config.PollInterval == 0 {
		config.PollInterval = 30 * time.Second
	}
	if config.MaxRetries == 0 {
		config.MaxRetries = 3
	}
	if config.RetryDelay == 0 {
		config.RetryDelay = 5 * time.Second
	}
	if config.ChunkSize == 0 {
		config.ChunkSize = 32 * 1024 // 32KB default
	}
	if config.ConcurrentFiles == 0 {
		config.ConcurrentFiles = 5
	}

	return &config, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: %s <config-file>\n", os.Args[0])
		os.Exit(1)
	}

	daemon, err := NewFileSyncDaemon(os.Args[1])
	if err != nil {
		log.Fatalf("Failed to create daemon: %v", err)
	}

	if err := daemon.Start(); err != nil {
		log.Fatalf("Daemon failed: %v", err)
	}
}
