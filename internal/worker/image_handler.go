package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/disintegration/imaging"

	"distributed-task-scheduler/internal/config"
	"distributed-task-scheduler/internal/models"
)

type imageUploader interface {
	Upload(ctx context.Context, key string, body []byte, contentType string) (string, error)
}

// ImageHandler processes resize/grayscale jobs for high-res images.
type ImageHandler struct {
	cfg        config.Config
	httpClient *http.Client
	local      imageUploader
	s3         imageUploader
}

// Image job payload accepted from the queue.
type imageJobPayload struct {
	SourceURL   string `json:"source_url"`
	OutputKey   string `json:"output_key"`
	Width       int    `json:"width"`
	Height      int    `json:"height"`
	Grayscale   bool   `json:"grayscale"`
	Destination string `json:"destination"`
}

// NewImageHandler constructs the handler and chooses an uploader (local or S3).
func NewImageHandler(ctx context.Context, cfg config.Config) (*ImageHandler, error) {
	timeout := cfg.ImageDownloadTimeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	baseDir := cfg.ImageOutputDir
	if baseDir == "" {
		baseDir = "./output"
	}

	var s3Upload imageUploader
	if cfg.ImageS3Bucket != "" {
		client, err := newS3Client(ctx, cfg)
		if err != nil {
			return nil, err
		}
		s3Upload = &s3Uploader{client: client, bucket: cfg.ImageS3Bucket}
	}

	return &ImageHandler{
		cfg: cfg,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		local: &localUploader{baseDir: baseDir},
		s3:    s3Upload,
	}, nil
}

func newS3Client(ctx context.Context, cfg config.Config) (*s3.Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.ImageS3Region),
	}
	if cfg.ImageS3Endpoint != "" {
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					URL:               cfg.ImageS3Endpoint,
					HostnameImmutable: cfg.ImageS3PathStyle,
					SigningRegion:     cfg.ImageS3Region,
					Source:            aws.EndpointSourceCustom,
				}, nil
			}
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})
		opts = append(opts, awsconfig.WithEndpointResolverWithOptions(resolver))
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.UsePathStyle = cfg.ImageS3PathStyle
	}), nil
}

// Handle downloads, transforms, and uploads a single image.
func (h *ImageHandler) Handle(ctx context.Context, job models.Job) error {
	payload, err := decodeImagePayload(job, h.cfg)
	if err != nil {
		return err
	}

	data, contentType, err := h.download(ctx, payload.SourceURL)
	if err != nil {
		return err
	}

	img, format, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("decode image: %w", err)
	}

	if payload.Grayscale {
		img = imaging.Grayscale(img)
	}

	width, height := payload.Width, payload.Height
	if width == 0 && height == 0 {
		width = h.cfg.ImageDefaultWidth
		height = h.cfg.ImageDefaultHeight
	}
	if width == 0 && height == 0 {
		width = 320
	}

	img = imaging.Resize(img, width, height, imaging.Lanczos)

	outputFormat := chooseFormat(payload.OutputKey, format, contentType)
	buf := &bytes.Buffer{}
	if err := imaging.Encode(buf, img, outputFormat, imaging.JPEGQuality(85)); err != nil {
		return fmt.Errorf("encode image: %w", err)
	}

	outputKey := payload.OutputKey
	if outputKey == "" {
		outputKey = fmt.Sprintf("%s.%s", job.ID, formatExtension(outputFormat))
	}
	outputKey = sanitizeKey(outputKey)

	uploader, err := h.pickUploader(payload.Destination)
	if err != nil {
		return err
	}

	_, err = uploader.Upload(ctx, outputKey, buf.Bytes(), mimeForFormat(outputFormat, contentType))
	if err != nil {
		return fmt.Errorf("upload: %w", err)
	}

	return nil
}

func (h *ImageHandler) download(ctx context.Context, url string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", fmt.Errorf("build request: %w", err)
	}
	resp, err := h.httpClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("download image: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		return nil, "", fmt.Errorf("download image: status %d", resp.StatusCode)
	}

	limit := h.cfg.ImageMaxBytes
	if limit == 0 {
		limit = 25 * 1024 * 1024
	}
	limited := io.LimitReader(resp.Body, limit+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, "", fmt.Errorf("read image: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, "", fmt.Errorf("image too large (>%d bytes)", limit)
	}

	return body, resp.Header.Get("Content-Type"), nil
}

func decodeImagePayload(job models.Job, cfg config.Config) (imageJobPayload, error) {
	payload := imageJobPayload{
		Grayscale: true,
		Width:     cfg.ImageDefaultWidth,
		Height:    cfg.ImageDefaultHeight,
	}
	raw, err := json.Marshal(job.Payload)
	if err != nil {
		return payload, fmt.Errorf("marshal payload: %w", err)
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return payload, fmt.Errorf("decode payload: %w", err)
	}
	if payload.SourceURL == "" {
		return payload, errors.New("source_url is required")
	}
	if payload.Width == 0 && payload.Height == 0 {
		payload.Width = cfg.ImageDefaultWidth
		payload.Height = cfg.ImageDefaultHeight
	}
	if payload.Width == 0 && payload.Height == 0 {
		payload.Width = 320
	}
	if payload.Destination == "" {
		if cfg.ImageS3Bucket != "" {
			payload.Destination = "s3"
		} else {
			payload.Destination = "local"
		}
	}
	return payload, nil
}

func (h *ImageHandler) pickUploader(destination string) (imageUploader, error) {
	switch strings.ToLower(destination) {
	case "s3":
		if h.s3 != nil {
			return h.s3, nil
		}
		return nil, errors.New("destination s3 requested but IMAGE_S3_BUCKET is not configured")
	case "local", "":
		if h.local != nil {
			return h.local, nil
		}
	}
	if h.s3 != nil {
		return h.s3, nil
	}
	if h.local != nil {
		return h.local, nil
	}
	return nil, errors.New("no uploader configured")
}

func formatExtension(format imaging.Format) string {
	switch format {
	case imaging.PNG:
		return "png"
	case imaging.GIF:
		return "gif"
	case imaging.TIFF:
		return "tiff"
	default:
		return "jpg"
	}
}

func chooseFormat(outputKey, decodeFormat, contentType string) imaging.Format {
	switch strings.ToLower(filepath.Ext(outputKey)) {
	case ".png":
		return imaging.PNG
	case ".jpg", ".jpeg":
		return imaging.JPEG
	}
	switch strings.ToLower(decodeFormat) {
	case "png":
		return imaging.PNG
	case "gif":
		return imaging.GIF
	case "tiff":
		return imaging.TIFF
	}
	if strings.Contains(strings.ToLower(contentType), "png") {
		return imaging.PNG
	}
	return imaging.JPEG
}

func mimeForFormat(format imaging.Format, fallback string) string {
	switch format {
	case imaging.PNG:
		return "image/png"
	case imaging.GIF:
		return "image/gif"
	case imaging.TIFF:
		return "image/tiff"
	default:
		if strings.Contains(strings.ToLower(fallback), "png") {
			return "image/png"
		}
		return "image/jpeg"
	}
}

func sanitizeKey(key string) string {
	key = filepath.Clean(key)
	key = strings.TrimPrefix(key, string(filepath.Separator))
	key = strings.TrimPrefix(key, "./")
	return key
}

type localUploader struct {
	baseDir string
}

func (l *localUploader) Upload(_ context.Context, key string, body []byte, _ string) (string, error) {
	path := filepath.Join(l.baseDir, key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", fmt.Errorf("create dirs: %w", err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return "", fmt.Errorf("write file: %w", err)
	}
	return path, nil
}

type s3Uploader struct {
	client *s3.Client
	bucket string
}

func (s *s3Uploader) Upload(ctx context.Context, key string, body []byte, contentType string) (string, error) {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return "", fmt.Errorf("put object: %w", err)
	}
	return fmt.Sprintf("s3://%s/%s", s.bucket, key), nil
}
