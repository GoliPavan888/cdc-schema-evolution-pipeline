$ErrorActionPreference = "Stop"

function Assert-LastExit([string]$step) {
  if ($LASTEXITCODE -ne 0) {
    throw "$step failed (exit code $LASTEXITCODE)"
  }
}

# Prefer shell env, then fallback to .env file for app credentials.
$dbUser = $env:MYSQL_USER
$dbPw = $env:MYSQL_PASSWORD
if (([string]::IsNullOrWhiteSpace($dbUser) -or [string]::IsNullOrWhiteSpace($dbPw)) -and (Test-Path ".env")) {
  $userLine = Select-String -Path ".env" -Pattern '^MYSQL_USER=(.+)$' | Select-Object -First 1
  $pwLine = Select-String -Path ".env" -Pattern '^MYSQL_PASSWORD=(.+)$' | Select-Object -First 1
  if ($userLine) { $dbUser = $userLine.Matches[0].Groups[1].Value }
  if ($pwLine) { $dbPw = $pwLine.Matches[0].Groups[1].Value }
}
if ([string]::IsNullOrWhiteSpace($dbUser) -or [string]::IsNullOrWhiteSpace($dbPw)) {
  throw "MYSQL_USER / MYSQL_PASSWORD are not set in environment or .env"
}
$dbUser = $dbUser.Trim()
$dbPw = $dbPw.Trim()

Write-Host "Checking required files..."
$requiredFiles = @(
  "docker-compose.yml",
  ".env.example",
  "output/lineage_report.json"
)

foreach ($file in $requiredFiles) {
  if (-not (Test-Path $file)) {
    throw "Missing required file: $file"
  }
}

Write-Host "Checking required services and healthchecks..."
$compose = Get-Content docker-compose.yml -Raw
$requiredServices = @("mysql:", "zookeeper:", "kafka:", "connect:", "processor:")
foreach ($svc in $requiredServices) {
  if ($compose -notmatch [Regex]::Escape($svc)) {
    throw "Missing service in compose: $svc"
  }
}
if (($compose -split "healthcheck:").Count -lt 6) {
  throw "Expected healthchecks for all services"
}

Write-Host "Starting stack..."
docker compose up -d --build
Assert-LastExit "docker compose up"

Write-Host "Waiting for MySQL seed check..."
Start-Sleep -Seconds 20

$customers = docker compose exec -T mysql mysql -h 127.0.0.1 -P 3306 "-u$($dbUser)" "-p$($dbPw)" -Nse "SELECT COUNT(*) FROM inventory.customers;"
Assert-LastExit "customers seed count query"
$products  = docker compose exec -T mysql mysql -h 127.0.0.1 -P 3306 "-u$($dbUser)" "-p$($dbPw)" -Nse "SELECT COUNT(*) FROM inventory.products;"
Assert-LastExit "products seed count query"
$orders    = docker compose exec -T mysql mysql -h 127.0.0.1 -P 3306 "-u$($dbUser)" "-p$($dbPw)" -Nse "SELECT COUNT(*) FROM inventory.orders;"
Assert-LastExit "orders seed count query"
$total = [int64]$customers + [int64]$products + [int64]$orders

Write-Host "customers=$customers products=$products orders=$orders total=$total"
if ($total -le 500000) {
  throw "Seeded row count is <= 500000"
}

Write-Host "Contract checks passed."
