param(
  [string]$HostName = "192.168.1.76",
  [string]$User = "root",
  [string]$RemoteDir = "/opt/a_share_quant_web"
)
$ErrorActionPreference = "Stop"
$LocalDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Write-Host "Uploading $LocalDir to $User@${HostName}:$RemoteDir"
ssh "$User@$HostName" "sudo mkdir -p $RemoteDir && sudo chown -R \$USER:\$USER $RemoteDir"
scp -r "$LocalDir\*" "$User@${HostName}:$RemoteDir/"
ssh "$User@$HostName" "cd $RemoteDir && chmod +x deploy_on_server.sh && ./deploy_on_server.sh"
Write-Host "Done. Visit: http://${HostName}:8766"
