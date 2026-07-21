# 提交 Flink 作业
Write-Host "=== Submitting Flink Job ===" -ForegroundColor Cyan

# Step 1: Copy jar from flink-job image to a named volume
docker run --name tmp-extract --rm docker-flink-job echo "extracting" 2>$null
docker create --name tmp-flink docker-flink-job 2>$null
docker cp tmp-flink:/opt/flink/jobs/flink-computation-1.0.0.jar C:\temp\flink-job.jar 2>$null
docker rm tmp-flink 2>$null

if (Test-Path C:\temp\flink-job.jar) {
    Write-Host "Jar extracted to C:\temp\flink-job.jar" -ForegroundColor Green

    # Remove old jar from jobmanager
    docker exec docker-jobmanager-1 sh -c "rm -f /tmp/job.jar" 2>$null

    # Copy to jobmanager
    docker cp C:\temp\flink-job.jar docker-jobmanager-1:/tmp/job.jar 2>$null

    if ($LASTEXITCODE -eq 0) {
        Write-Host "Jar copied to jobmanager" -ForegroundColor Green

        # Submit job
        $result = docker exec docker-jobmanager-1 flink run /tmp/job.jar 2>&1
        Write-Host $result
    }
} else {
    # Fallback: build jar locally
    Write-Host "Extracting failed, trying alternate method..." -ForegroundColor Yellow
    docker run --rm --network docker_default -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 docker-flink-job sh -c "/opt/flink/jobs/submit.sh" 2>&1
}

Write-Host "=== Done ===" -ForegroundColor Cyan
