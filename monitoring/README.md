# DittoFS Monitoring Stack

This directory contains a complete monitoring setup for DittoFS using Prometheus and Grafana.

## Quick Start

### 1. Start DittoFS with metrics enabled

Make sure DittoFS is running with the metrics endpoint on port 9090:

```bash
./dittofs start  # Metrics exposed on :9090/metrics by default
```

### 2. Start Prometheus and Grafana

```bash
cd monitoring
docker-compose up -d
```

This will start:
- **Prometheus** on http://localhost:9091 (scraping DittoFS metrics every 5s)
- **Grafana** on http://localhost:3000 (admin/admin)

### 3. Access Grafana

1. Open http://localhost:3000
2. Login with `admin` / `admin`
3. The **"DittoFS Upload Performance"** dashboard should be automatically loaded

## Dashboard Overview

The dashboard is organized into sections:

### 📊 Upload Performance Overview
- **Cache Write Speed (instant)**: Current throughput to cache (MB/s) using irate()
- **S3 Upload Speed (instant)**: Current throughput to S3 (MB/s) using irate()
- **Active Buffers**: Number of files currently cached
- **Flush Duration (p95)**: 95th percentile flush time

**Note**: Cache writes and S3 uploads happen at different times:
- Cache writes are fast (MB/s) and happen when clients write files
- S3 uploads are slower and happen during flushes (timeout-based or explicit)
- When idle, both metrics will show ~0

### 📈 Throughput Over Time
- **Cache vs S3 Throughput**: Time series graph showing both rates over time
  - Green line: Cache write throughput (spiky when files are written)
  - Blue line: S3 upload throughput (happens during flushes)
  - Legend shows mean and max values for the time range

### 🔄 S3 Flush Phase Breakdown
- Time series showing duration of each flush phase:
  - **Cache Read**: Time to read from cache
  - **S3 Upload**: Time to upload to S3
  - **Cache Clear**: Time to clear cache

### ⚡ Cache Performance
- **Cache Throughput**: Read/write MB/s
- **Cache Latency**: Read/write latency in microseconds (p95)

### 📈 Flush Operations
- **Operations by Reason**: Count of flushes by trigger:
  - `stable_write`: Explicit sync requested
  - `commit`: NFS COMMIT procedure
  - `timeout`: Auto-flush after idle period
  - `threshold`: Cache size threshold exceeded
- **Reasons Distribution**: Pie chart showing flush trigger breakdown

## Running Benchmarks with Monitoring

1. Start the monitoring stack:
```bash
cd monitoring && docker-compose up -d
```

2. Open Grafana dashboard at http://localhost:3000

3. Run your benchmark:
```bash
./scripts/benchmark_upload_v2.sh /tmp/nfstest
```

4. Watch the metrics in real-time:
   - Cache write speed should spike during file creation
   - S3 upload speed should show actual throughput
   - Flush phase breakdown shows where time is spent

## Metrics Available

### S3 Metrics
- `dittofs_s3_flush_phase_duration_seconds{phase}` - Duration of cache_read, s3_upload, cache_clear
- `dittofs_s3_flush_operations_total{reason,status}` - Count by trigger reason
- `dittofs_s3_flush_bytes{reason}` - Size distribution of flushes
- `dittofs_s3_operation_duration_seconds{operation}` - All S3 operations
- `dittofs_s3_bytes_transferred_total{operation}` - Bytes uploaded/downloaded

### Cache Metrics
- `dittofs_cache_write_operations_total{status}` - Write operation count
- `dittofs_cache_write_duration_seconds` - Write latency
- `dittofs_cache_write_bytes_total` - Total bytes written
- `dittofs_cache_read_operations_total{status}` - Read operation count
- `dittofs_cache_read_duration_seconds` - Read latency
- `dittofs_cache_read_bytes_total` - Total bytes read
- `dittofs_cache_size_bytes{content_id}` - Cache size per file
- `dittofs_cache_buffer_count` - Active buffer count
- `dittofs_cache_reset_operations_total` - Cache clears

## Customizing

### Change scrape interval
Edit `prometheus.yml`:
```yaml
global:
  scrape_interval: 5s  # Change to 1s for more granular data
```

### Add alerting
Create `prometheus/alerts.yml` and add alerting rules for:
- High flush latency
- S3 errors
- Cache size exceeding thresholds

### Create custom dashboards
1. Go to Grafana → Dashboards → New
2. Use the metrics listed above
3. Save and export JSON to `grafana/dashboards/`

## Troubleshooting

### Prometheus can't scrape DittoFS
- Check DittoFS is running: `curl http://localhost:9090/metrics`
- On macOS, Prometheus uses `host.docker.internal` to reach host services
- On Linux, you may need to change prometheus.yml to use your host IP

### No data in Grafana
- Check Prometheus targets: http://localhost:9091/targets
- DittoFS target should show "UP"
- Check Grafana datasource: Settings → Data Sources → Prometheus

### Dashboard doesn't load
- Restart Grafana: `docker-compose restart grafana`
- Check logs: `docker-compose logs grafana`

## Stopping

```bash
docker-compose down         # Stop containers
docker-compose down -v      # Stop and remove data volumes
```

## Architecture

```
┌──────────┐     :9090      ┌────────────┐
│ DittoFS  │────metrics────▶│ Prometheus │
└──────────┘                └──────┬─────┘
                                   │
                                   │ queries
                                   ▼
                            ┌─────────────┐
                            │   Grafana   │
                            └─────────────┘
                                 :3000
```

Prometheus scrapes DittoFS metrics every 5 seconds and stores them in a time-series database. Grafana queries Prometheus to visualize the metrics.
