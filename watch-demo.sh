#!/bin/bash
# Watch the distributed task scheduler in action

echo "🔍 Monitoring Worker Activity..."
echo "================================="
echo ""
echo "Upload an image at http://localhost:8080 to see workers in action!"
echo ""
echo "Press Ctrl+C to stop monitoring"
echo ""

# Follow worker logs and highlight important events
docker-compose logs -f worker 2>&1 | grep --line-buffered -E "(processing job|completed|failed|worker started)"
