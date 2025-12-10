package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/image/draw"

	"distributed-task-scheduler/internal/models"
)

// localResizePayload is the expected job payload for type image:resize.
type localResizePayload struct {
	Filepath    string `json:"filepath"`
	OutputPath  string `json:"output_path"`
	OutputFile  string `json:"output_filename"`
	RequestedBy string `json:"requested_by"`
}

// LocalResizeHandler resizes local images and writes a thumbnail.
type LocalResizeHandler struct {
	width int
	// sleep simulates heavy processing work.
	sleep time.Duration
}

// NewLocalResizeHandler builds a handler with sensible defaults.
func NewLocalResizeHandler() *LocalResizeHandler {
	return &LocalResizeHandler{
		width: 300,
		sleep: 5 * time.Second,
	}
}

// Handle processes a single image resize job.
func (h *LocalResizeHandler) Handle(ctx context.Context, job models.Job) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	payload, err := decodeLocalResizePayload(job)
	if err != nil {
		return err
	}

	// Simulate heavy load to demonstrate leasing behavior.
	time.Sleep(h.sleep)
	if err := ctx.Err(); err != nil {
		return err
	}

	in, err := os.Open(payload.Filepath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("source image missing: %w", err)
		}
		return fmt.Errorf("open source: %w", err)
	}
	defer in.Close()

	src, _, err := image.Decode(in)
	if err != nil {
		return fmt.Errorf("decode image: %w", err)
	}

	if src.Bounds().Dx() == 0 || src.Bounds().Dy() == 0 {
		return errors.New("invalid image dimensions")
	}

	newWidth := h.width
	newHeight := int(float64(src.Bounds().Dy()) * float64(newWidth) / float64(src.Bounds().Dx()))
	if newHeight == 0 {
		newHeight = newWidth
	}

	dst := image.NewRGBA(image.Rect(0, 0, newWidth, newHeight))
	draw.CatmullRom.Scale(dst, dst.Bounds(), src, src.Bounds(), draw.Over, nil)

	if err := os.MkdirAll(filepath.Dir(payload.OutputPath), 0o755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}

	out, err := os.Create(payload.OutputPath)
	if err != nil {
		return fmt.Errorf("create output file: %w", err)
	}
	defer out.Close()

	switch strings.ToLower(filepath.Ext(payload.OutputPath)) {
	case ".png":
		if err := png.Encode(out, dst); err != nil {
			return err
		}
	default:
		if err := jpeg.Encode(out, dst, &jpeg.Options{Quality: 85}); err != nil {
			return err
		}
	}

	return nil
}

func decodeLocalResizePayload(job models.Job) (localResizePayload, error) {
	payload := localResizePayload{}
	raw, err := json.Marshal(job.Payload)
	if err != nil {
		return payload, fmt.Errorf("marshal payload: %w", err)
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return payload, fmt.Errorf("decode payload: %w", err)
	}
	if payload.Filepath == "" {
		return payload, errors.New("filepath is required")
	}
	if payload.OutputPath == "" {
		file := filepath.Base(payload.Filepath)
		payload.OutputPath = filepath.Join(filepath.Dir(payload.Filepath), "thumb_"+file)
	}
	return payload, nil
}
