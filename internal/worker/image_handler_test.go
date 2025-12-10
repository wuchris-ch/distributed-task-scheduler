package worker

import (
	"bytes"
	"context"
	"image"
	"image/color"
	"image/png"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"distributed-task-scheduler/internal/config"
	"distributed-task-scheduler/internal/models"
)

func TestImageHandler_LocalResizeAndGrayscale(t *testing.T) {
	img := image.NewRGBA(image.Rect(0, 0, 10, 10))
	// Paint red so we can verify grayscale output has equal channels.
	for y := 0; y < 10; y++ {
		for x := 0; x < 10; x++ {
			img.Set(x, y, color.RGBA{R: 255, G: 0, B: 0, A: 255})
		}
	}
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(buf.Bytes())
	}))
	defer srv.Close()

	tempDir := t.TempDir()
	cfg := config.Config{
		ImageOutputDir:       tempDir,
		ImageDownloadTimeout: 2 * time.Second,
		ImageMaxBytes:        2 * 1024 * 1024,
		ImageDefaultWidth:    5,
	}

	handler, err := NewImageHandler(context.Background(), cfg)
	if err != nil {
		t.Fatalf("new image handler: %v", err)
	}

	job := models.Job{
		ID:   "job-1",
		Type: "resize_image",
		Payload: map[string]any{
			"source_url": srv.URL,
			"grayscale":  true,
			"width":      5,
			"output_key": "thumbs/test.png",
		},
	}

	if err := handler.Handle(context.Background(), job); err != nil {
		t.Fatalf("handle image: %v", err)
	}

	outputPath := filepath.Join(tempDir, "thumbs", "test.png")
	data, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("output not written: %v", err)
	}

	outImg, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("decode output: %v", err)
	}

	if outImg.Bounds().Dx() != 5 {
		t.Fatalf("expected width 5, got %d", outImg.Bounds().Dx())
	}
	r, g, b, _ := outImg.At(0, 0).RGBA()
	if r != g || g != b {
		t.Fatalf("expected grayscale pixel, got r=%d g=%d b=%d", r, g, b)
	}
}
