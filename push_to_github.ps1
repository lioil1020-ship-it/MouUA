# 使用方法：在安裝 Git 後，以 PowerShell 執行此檔案（在 E:\py\ModUA 目錄下）
# 先確認已設定 git user.name 與 user.email，且已登入 GitHub 或已設定 SSH key

Set-Location -Path "$PSScriptRoot"

Write-Host "== 初始化 Git 倉庫（若尚未初始化） =="
if (-not (Test-Path .git)) {
    git init
} else {
    Write-Host ".git 已存在，跳過 git init"
}

Write-Host "== 新增 .gitignore 並加入所有檔案 =="
git add .gitignore
git add .

Write-Host "== 嘗試提交（若無變更則跳過） =="
try {
    git commit -m 'Initial commit' -q
    Write-Host "Commit 完成。"
} catch {
    Write-Host "沒有可提交的變更或 commit 失敗： $_"
}

Write-Host "== 設定主分支名稱為 main =="
git branch -M main

Write-Host "== 設定 remote origin 並推送 =="
$remoteUrl = 'https://github.com/lioil1020-ship-it/MouUA.git'
try {
    git remote remove origin 2>$null
} catch { }
git remote add origin $remoteUrl

Write-Host "準備推送到 $remoteUrl -- 可能會要求輸入 GitHub 認證（PAT）或使用 SSH。"
git push -u origin main

Write-Host "== 顯示遠端與最近提交資訊 =="
git remote -v
git log -1 --pretty=oneline
git status

Write-Host "執行結束。若推送失敗，請檢查 Git 是否已登入或使用 PAT/SSH。"
