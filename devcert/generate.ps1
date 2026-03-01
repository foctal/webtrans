Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

$certName = "localhost"
$days = 10
$conf = "openssl.conf"

Write-Host "==> Generating self-signed certificate ($certName)"
Write-Host "    validity: $days days"
Write-Host "    config:   $conf"

$openssl = if ($env:OPENSSL_BIN) { $env:OPENSSL_BIN } else { "openssl" }
if (-not (Get-Command $openssl -ErrorAction SilentlyContinue)) {
    throw "openssl command not found. Set OPENSSL_BIN to openssl.exe path or add openssl to PATH."
}

# Generate ECDSA P-256 private key
& $openssl ecparam `
    -genkey `
    -name prime256v1 `
    -out "$certName.key"

if ($LASTEXITCODE -ne 0) {
    throw "Failed to generate private key."
}

# Generate self-signed certificate
& $openssl req `
    -x509 `
    -sha256 `
    -nodes `
    -days "$days" `
    -key "$certName.key" `
    -out "$certName.crt" `
    -config "$conf" `
    -extensions v3_req

if ($LASTEXITCODE -ne 0) {
    throw "Failed to generate certificate."
}

# Generate raw SHA-256 hash (DER -> hex, no colons)
$derPath = "$certName.der"
& $openssl x509 `
    -in "$certName.crt" `
    -outform der `
    -out $derPath

if ($LASTEXITCODE -ne 0) {
    throw "Failed to export certificate in DER format."
}

$hex = (Get-FileHash -Path $derPath -Algorithm SHA256).Hash.ToLowerInvariant()
Set-Content -Path "$certName.hex" -Value $hex -NoNewline
Remove-Item $derPath -Force

# Also print human-readable fingerprint
& $openssl x509 `
    -in "$certName.crt" `
    -noout `
    -fingerprint `
    -sha256 `
    > "$certName.fingerprint"

if ($LASTEXITCODE -ne 0) {
    throw "Failed to generate fingerprint."
}

Write-Host "==> Done"
Write-Host "  - $certName.crt"
Write-Host "  - $certName.key"
Write-Host "  - $certName.hex      (for serverCertificateHashes)"
Write-Host "  - $certName.fingerprint"
