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

Write-Host "Applying schema change 1 (rename column)..."
docker compose exec -T mysql mysql -h 127.0.0.1 -P 3306 "-u$($dbUser)" "-p$($dbPw)" -D inventory -e "ALTER TABLE products RENAME COLUMN description TO product_description;"
Assert-LastExit "Schema change 1"

docker compose exec -T mysql mysql -h 127.0.0.1 -P 3306 "-u$($dbUser)" "-p$($dbPw)" -D inventory -e "INSERT INTO products (name, product_description, price) VALUES ('Schema Test Product', 'renamed column works', 99.99);"
Assert-LastExit "Schema change 1 validation insert"

Start-Sleep -Seconds 10

Write-Host "Applying schema change 2 (add NOT NULL column with default)..."
docker compose exec -T mysql mysql -h 127.0.0.1 -P 3306 "-u$($dbUser)" "-p$($dbPw)" -D inventory -e "ALTER TABLE customers ADD COLUMN country_code VARCHAR(3) NOT NULL DEFAULT 'USA';"
Assert-LastExit "Schema change 2"

docker compose exec -T mysql mysql -h 127.0.0.1 -P 3306 "-u$($dbUser)" "-p$($dbPw)" -D inventory -e "INSERT INTO customers (first_name, last_name, email, created_at) VALUES ('Schema', 'Tester', CONCAT('schema.', UNIX_TIMESTAMP(), '@example.com'), NOW());"
Assert-LastExit "Schema change 2 validation insert"

Start-Sleep -Seconds 20

Write-Host "Triggering lineage report generation..."
docker compose exec -T processor python -m main lineage
Assert-LastExit "Lineage generation"

Write-Host "Schema evolution simulation completed."
