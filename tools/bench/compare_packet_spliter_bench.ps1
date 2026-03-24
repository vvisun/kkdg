param(
    [string]$BaseRef = "HEAD~1",
    [int]$Count = 8,
    [string]$BenchTime = "200ms",
    [string]$Package = "./kknet/kkprocessor",
    [string]$BenchRegex = "^BenchmarkPacketSpliter_"
)

$ErrorActionPreference = "Stop"

function Get-BenchstatPath {
    if (Get-Command benchstat -ErrorAction SilentlyContinue) {
        return "benchstat"
    }

    Write-Host "benchstat not found; installing golang.org/x/perf/cmd/benchstat@latest ..."
    go install golang.org/x/perf/cmd/benchstat@latest

    $gopath = (go env GOPATH).Trim()
    if ([string]::IsNullOrWhiteSpace($gopath)) {
        throw "GOPATH is empty; cannot locate benchstat."
    }

    $candidate = Join-Path $gopath "bin\benchstat.exe"
    if (Test-Path $candidate) {
        return $candidate
    }

    throw "benchstat install succeeded but binary not found at $candidate"
}

function Invoke-BenchRun {
    param(
        [string]$WorkDir,
        [string]$OutFile
    )
    $cmd = "go test $Package -run ^$ -bench $BenchRegex -benchmem -count $Count -benchtime $BenchTime"
    Write-Host "Running in $WorkDir"
    Write-Host "  $cmd"
    Push-Location $WorkDir
    try {
        Invoke-Expression $cmd | Out-File -FilePath $OutFile -Encoding utf8
    }
    finally {
        Pop-Location
    }
}

$repoRoot = (Resolve-Path ".").Path
$tmpRoot = Join-Path $env:TEMP ("kkdg_packet_spliter_bench_" + [guid]::NewGuid().ToString("N"))
$baseWorktree = Join-Path $tmpRoot "base"
$resultDir = Join-Path $tmpRoot "results"
New-Item -ItemType Directory -Path $tmpRoot | Out-Null
New-Item -ItemType Directory -Path $resultDir | Out-Null

$baseOut = Join-Path $resultDir "base.txt"
$curOut = Join-Path $resultDir "current.txt"

try {
    $benchstatCmd = Get-BenchstatPath

    Write-Host "Creating temporary worktree for $BaseRef ..."
    git -C $repoRoot worktree add --detach $baseWorktree $BaseRef | Out-Null

    Invoke-BenchRun -WorkDir $baseWorktree -OutFile $baseOut
    Invoke-BenchRun -WorkDir $repoRoot -OutFile $curOut

    Write-Host ""
    Write-Host "=== benchstat ($BaseRef -> current) ==="
    & $benchstatCmd $baseOut $curOut
    Write-Host ""
    Write-Host "Raw benchmark outputs:"
    Write-Host "  base   : $baseOut"
    Write-Host "  current: $curOut"
}
finally {
    if (Test-Path $baseWorktree) {
        git -C $repoRoot worktree remove --force $baseWorktree | Out-Null
    }
}

