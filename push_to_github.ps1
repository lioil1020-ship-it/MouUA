#!/usr/bin/env pwsh
# Usage: run this PowerShell script from the project root to push changes to GitHub,
# then optionally create a GitHub release and issue using `gh` or a Personal Access Token
# (set `GITHUB_TOKEN` in environment variables).

Set-Location -Path "$PSScriptRoot"

Write-Host "== Initialize git repository (if needed) =="
if (-not (Test-Path .git)) {
    git init
} else {
    Write-Host ".git exists, skipping git init"
}

Write-Host "== Add files to index =="
git add .gitignore
git add .

Write-Host "== Try to commit (will skip if no changes) =="
try {
    git commit -m 'Update repository' -q
    Write-Host "Commit completed."
} catch {
    Write-Host "No changes to commit or commit failed: $_"
}

Write-Host "== Ensure main branch name =="
git branch -M main

Write-Host "== Configure remote origin and push =="
$remoteUrl = 'https://github.com/lioil1020-ship-it/MouUA.git'
try {
    git remote remove origin 2>$null
} catch { }
git remote add origin $remoteUrl

Write-Host "Pushing to $remoteUrl (you may be prompted for credentials)..."
git push -u origin main

Write-Host "== Remote and recent commit info =="
git remote -v
git log -1 --pretty=oneline
git status

Write-Host "== Create and push tag (vYYYYMMDD) =="
$tag = 'v' + (Get-Date -Format yyyyMMdd)
Write-Host "Using tag: $tag"
# Create annotated tag if it doesn't exist
try {
    $exists = git rev-parse -q --verify "refs/tags/$tag" 2>$null
} catch { $exists = $null }
if (-not $exists) {
    git tag -a $tag -m "Release $tag"
    git push origin $tag
    Write-Host "Tag $tag created and pushed."
} else {
    Write-Host "Tag $tag already exists, skipping creation."
}

Write-Host "== Create GitHub release / issue (prefer gh, fallback to API if GITHUB_TOKEN set) =="
# Prepare release notes
$short = (git rev-parse --short HEAD)
$files = git show --name-only --pretty="" HEAD | Where-Object { $_ -ne "" } | ForEach-Object { "- $_" } | Out-String
$releaseNotes = "Auto-release of commit $short`n`nFiles:`n$files"

# Parse owner/repo from remote URL if possible
$owner = 'lioil1020-ship-it'
$repo = 'MouUA'
if ($remoteUrl -match 'github\.com[:/](.+?)/(.+?)(?:\.git)?$') {
    $owner = $matches[1]
    $repo = $matches[2]
}

if (Get-Command gh -ErrorAction SilentlyContinue) {
    Write-Host "gh detected — creating release and issue via gh"
    try {
        gh release create $tag --title $tag --notes $releaseNotes
        Write-Host "Release $tag created via gh."
    } catch {
        Write-Host "gh release create failed: $_"
    }
    try {
        gh issue create --title "Release $tag" --body $releaseNotes
        Write-Host "Issue created via gh."
    } catch {
        Write-Host "gh issue create failed: $_"
    }
} elseif ($env:GITHUB_TOKEN) {
    Write-Host "gh not found, using GITHUB_TOKEN to call GitHub API"
    $apiUrl = "https://api.github.com/repos/$owner/$repo"
    $headers = @{ Authorization = "token $($env:GITHUB_TOKEN)"; 'User-Agent' = 'powershell' }

    # Create release if not existing
    try {
        $existing = Invoke-RestMethod -Uri "$apiUrl/releases/tags/$tag" -Headers $headers -Method Get -ErrorAction SilentlyContinue
    } catch { $existing = $null }
    if ($existing) {
        Write-Host "Release for $tag already exists on GitHub."
    } else {
        $body = @{ tag_name = $tag; name = $tag; body = $releaseNotes; draft = $false; prerelease = $false } | ConvertTo-Json -Depth 5
        try {
            $resp = Invoke-RestMethod -Uri "$apiUrl/releases" -Headers $headers -Method Post -Body $body -ContentType 'application/json'
            Write-Host "Release $tag created via GitHub API."
        } catch {
            Write-Host "Failed to create release via API: $_"
        }
    }

    # Create an issue to record the release
    $issueBody = @{ title = "Release $tag"; body = $releaseNotes } | ConvertTo-Json -Depth 5
    try {
        $issueResp = Invoke-RestMethod -Uri "$apiUrl/issues" -Headers $headers -Method Post -Body $issueBody -ContentType 'application/json'
        Write-Host "Issue created via GitHub API: #$($issueResp.number)"
    } catch {
        Write-Host "Failed to create issue via API: $_"
    }
} else {
    Write-Host "Neither gh CLI nor GITHUB_TOKEN available — skipping release/issue creation."
    Write-Host "To enable automatic release creation, install gh and run 'gh auth login', or set GITHUB_TOKEN env var."
}

Write-Host "Done. If push or release creation failed, check git authentication or GITHUB_TOKEN/gh configuration."
