param(
    [Parameter(Mandatory = $true)]
    [string]$TargetPath,

    [Parameter(Mandatory = $false)]
    [string]$RemoteUrl = ""
)

$ErrorActionPreference = "Stop"

Write-Host "[1/6] Creating target directory..."
New-Item -ItemType Directory -Path $TargetPath -Force | Out-Null

Write-Host "[2/6] Creating monorepo folders..."
$folders = @(
    "apps/web",
    "apps/api",
    "packages/contracts",
    "packages/domain",
    "infra",
    ".github/workflows"
)
foreach ($folder in $folders) {
    New-Item -ItemType Directory -Path (Join-Path $TargetPath $folder) -Force | Out-Null
}

Write-Host "[3/6] Writing baseline files..."
@"
# MonteCarlo Risk Platform

Modern greenfield architecture:
- apps/web (Next.js)
- apps/api (FastAPI)
- packages/contracts
- packages/domain
- infra
"@ | Set-Content -Path (Join-Path $TargetPath "README.md") -Encoding UTF8

@"
# Python
**/__pycache__/
**/.pytest_cache/
**/.venv/

# Node
**/node_modules/
**/.next/

# Env
.env
.env.*

# OS/IDE
.DS_Store
Thumbs.db
.vscode/
.idea/
"@ | Set-Content -Path (Join-Path $TargetPath ".gitignore") -Encoding UTF8

@"
root = true

[*]
charset = utf-8
end_of_line = lf
insert_final_newline = true
indent_style = space
indent_size = 2
trim_trailing_whitespace = true
"@ | Set-Content -Path (Join-Path $TargetPath ".editorconfig") -Encoding UTF8

Write-Host "[4/6] Initializing git repository..."
Push-Location $TargetPath
if (-not (Test-Path ".git")) {
    git init | Out-Null
}

git checkout -B main | Out-Null
git add .

try {
    git commit -m "chore: bootstrap monorepo" | Out-Null
} catch {
    Write-Host "No changes to commit yet."
}

Write-Host "[5/6] Configuring remote (optional)..."
if ($RemoteUrl -and $RemoteUrl.Trim().Length -gt 0) {
    $hasOrigin = git remote | Select-String -Pattern "^origin$"
    if (-not $hasOrigin) {
        git remote add origin $RemoteUrl
    } else {
        git remote set-url origin $RemoteUrl
    }
}

Write-Host "[6/6] Done."
Write-Host "Repository ready at: $TargetPath"
if ($RemoteUrl) {
    Write-Host "Next: git push -u origin main"
}
Pop-Location
