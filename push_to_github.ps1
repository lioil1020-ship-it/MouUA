#!/usr/bin/env pwsh
# Usage: run this PowerShell script from the project root to push changes to GitHub.
# Make sure git is installed and you have configured user.name/user.email and authentication (PAT or SSH).

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

Write-Host "Done. If push failed, check git authentication (PAT or SSH)."
