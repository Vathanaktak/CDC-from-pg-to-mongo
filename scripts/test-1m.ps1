# CDC Benchmark: Testing 1,000,000 Records Replication
Set-Location "$PSScriptRoot\.."

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "    CDC Benchmark: Testing 1,000,000 Records Replication   " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

Write-Host "Step 1: Inserting 1,000,000 rows into PostgreSQL..." -ForegroundColor Yellow
$startTime = Get-Date

docker compose exec -T postgres psql -U postgres -d demo -c "INSERT INTO customers (name, email, city) SELECT 'Customer_' || i, 'customer_' || i || '@example.com', (ARRAY['Phnom Penh', 'Siem Reap', 'Battambang', 'Kampot', 'Kandal'])[1 + (i % 5)] FROM generate_series(1, 1000000) AS i;"

$pgDuration = ((Get-Date) - $startTime).TotalSeconds
Write-Host "-> PostgreSQL insert done in $([math]::Round($pgDuration)) seconds." -ForegroundColor Green
Write-Host ""
Write-Host "Step 2: Tracking replication progress into MongoDB..." -ForegroundColor Yellow

do {
    try {
        $rawCount = (docker compose exec -T mongo mongosh --quiet demo --eval "db.customers.countDocuments()").Trim()
        if ($rawCount -match '^\d+$') {
            $count = [int]$rawCount
            $percent = [math]::Round(($count / 1000000) * 100, 1)
            Write-Host "[$((Get-Date).ToString('HH:mm:ss'))] Replicated: $count / 1,000,000 ($percent%)" -ForegroundColor Cyan
            if ($count -ge 1000000) { break }
        }
    } catch {
        # ignore transient read error
    }
    Start-Sleep -Seconds 3
} while ($true)

$totalSeconds = ((Get-Date) - $startTime).TotalSeconds
$throughput = [math]::Round(1000000 / $totalSeconds)

Write-Host "==========================================================" -ForegroundColor Green
Write-Host " SUCCESS: 1,000,000 records synced in $([math]::Round($totalSeconds)) seconds!" -ForegroundColor Green
Write-Host " Average throughput: $throughput records/sec" -ForegroundColor Green
Write-Host "==========================================================" -ForegroundColor Green
