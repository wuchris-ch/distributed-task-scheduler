# 🎮 Interactive Demo Guide

## What This System Actually Does

The distributed task scheduler processes images through these steps:

1. **Resize**: Shrinks to 320x320 pixels (configurable)
2. **Grayscale**: Converts to black & white
3. **Distribute**: Multiple workers compete to process jobs
4. **Fault-tolerant**: If a worker crashes, another picks up the work

## Expected Visual Results

### ❌ What You Should NOT See:
- Original colored image in the "Result" column

### ✅ What You SHOULD See:
- **Black and white (grayscale) thumbnail** of your original image
- The image should be noticeably smaller (320x320 pixels)
- Status changing: `queued` → `in_progress` → `completed`

## How to Watch the Demo

### 1. Monitor Workers in Real-Time

**Terminal 1: Watch Worker Logs**
```bash
./watch-demo.sh
```
or
```bash
docker-compose logs -f worker | grep -E "(processing|completed|failed)"
```

### 2. Upload an Image

**In Browser:** http://localhost:8080
- Click "Choose File"
- Select a large, colorful image (>1 MB)
- Click "Start Job"

### 3. What to Observe

**In the Browser:**
- New row appears in Job Table
- Status starts as "queued"
- Refreshes to "in_progress" 
- Changes to "completed"
- Result shows a BLACK & WHITE thumbnail

**In Terminal (Worker Logs):**
```
worker_2 | processing job 2ab2b91a-2da9-47f5-a17f-81d6e601cd06
worker_2 | job completed successfully
```

This tells you **which worker** processed your job!

## 🎯 Chaos Engineering Demo

The REAL power is fault tolerance. Try this:

### Step 1: Upload a Large Image
Choose a big file (5+ MB) so processing takes a few seconds

### Step 2: Kill a Worker Mid-Processing
**While status is "in_progress"**, kill the worker:
```bash
# Find which worker is processing
docker-compose ps

# Kill it (replace worker_2 with the active one)
docker stop distributed-task-scheduler_worker_2

# Watch another worker pick it up!
docker-compose logs -f worker
```

### Step 3: Observe Automatic Recovery
- After ~30 seconds (visibility timeout)
- Another worker automatically claims the job
- Job completes successfully despite the failure!

### Step 4: Restore the Worker
```bash
docker-compose up -d --scale worker=3
```

## 📊 Check System Metrics

```bash
# See job statistics
curl http://localhost:8080/metrics | grep tasks_

# View all completed jobs
curl http://localhost:8080/status | jq
```

## 🐛 Troubleshooting

### Result is NOT Grayscale?
Check the actual file:
```bash
# List processed images
ls -lh uploads/

# View a specific result
open uploads/thumb_your-image-name.jpg
```

### Workers Not Processing?
```bash
# Check worker status
docker-compose ps

# View full worker logs
docker-compose logs worker
```

### Jobs Stuck in "queued"?
```bash
# Check Redis queue
docker-compose exec redis redis-cli LLEN ready:medium
```

## 🎓 What Makes This Impressive

### Technical Highlights:
1. **Distributed Locking**: Multiple workers don't process the same job twice
2. **Visibility Timeout**: Failed jobs automatically get retried
3. **Horizontal Scaling**: Add workers with `--scale worker=N`
4. **Zero Downtime**: Kill workers and jobs still complete
5. **Observability**: Real-time metrics and logging

### The Value Isn't the Visual Output
The value is in the **architecture**, not the image transformation:
- ✅ Handles job queuing and distribution
- ✅ Prevents race conditions with atomic Redis operations
- ✅ Automatically retries failed jobs
- ✅ Scales horizontally
- ✅ Production-ready error handling

This is similar to how companies like:
- **Airbnb** processes uploaded photos
- **Netflix** encodes videos  
- **Shopify** processes orders

The "boring" grayscale demo could easily be replaced with:
- Video transcoding
- Email sending
- Data pipeline processing
- AI/ML model inference
- PDF generation

## 🚀 Next Steps

Want to make it more visual? Consider:
1. Add multiple transformation options (blur, sepia, edges)
2. Show before/after comparison in the UI
3. Add progress bars for long-running jobs
4. Implement WebSocket for real-time updates instead of polling
