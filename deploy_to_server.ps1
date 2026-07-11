param(
  [Parameter(Mandatory=$true)][string]$HostName,
  [string]$User = "root",
  [string]$RemoteDir = "/opt/a_share_quant_web",
  [int]$SshPort = 22
)
$ErrorActionPreference = "Stop"
$LocalDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$files = @(
  "app.py", "a_share_daily.py", "requirements.txt", "quant-web.service",
  "deploy_on_server.sh", "opening_watchlist.example.json", "quant-web.env.example"
)
Write-Host "1/3 创建远程目录 $RemoteDir"
ssh -p $SshPort "$User@$HostName" "mkdir -p '$RemoteDir'"
Write-Host "2/3 上传源码和部署配置"
foreach ($file in $files) {
  scp -P $SshPort (Join-Path $LocalDir $file) "${User}@${HostName}:${RemoteDir}/"
}
Write-Host "3/3 安装依赖并启动 systemd 服务"
ssh -p $SshPort "$User@$HostName" "cd '$RemoteDir' && chmod +x deploy_on_server.sh && ./deploy_on_server.sh"
Write-Host "部署完成：http://${HostName}:8766/"
