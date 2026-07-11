# A股量化推荐网站

这是把 `a_share_daily.py` 包装成的一个轻量网站，使用 Python 标准库 `http.server`，不依赖 Flask/Nginx 也可直接运行。

## 功能

- 首页展示最新交易日量化结果；
- 给出“今日首选关注”和推荐优先级；
- 展示评分、战法、买入区间、止损、目标价、风险收益比、入选原因；
- 可点击“立即运行量化扫描”；
- 后台按北京时间工作日 `15:35,21:00` 自动扫描；
- 保留历史报告和 CSV；
- 可选 `QUANT_WEB_TOKEN` 保护手动扫描接口。

## 本地启动

```powershell
cd C:\Users\baoyu\Documents\Codex\2026-07-11\bia\outputs\a_share_quant_web
python app.py --host 0.0.0.0 --port 8766
```

访问：

```text
http://127.0.0.1:8766
```

## Linux 服务器部署

假设服务器是 Ubuntu/Debian，目标目录 `/opt/a_share_quant_web`，端口 `8766`：

```bash
sudo mkdir -p /opt/a_share_quant_web
sudo chown -R $USER:$USER /opt/a_share_quant_web
# 把本目录所有文件上传到 /opt/a_share_quant_web
cd /opt/a_share_quant_web
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
python app.py --host 0.0.0.0 --port 8766
```

## systemd 常驻服务

```bash
sudo cp quant-web.service /etc/systemd/system/quant-web.service
sudo systemctl daemon-reload
sudo systemctl enable --now quant-web
sudo systemctl status quant-web
```

访问：

```text
http://服务器IP:8766
```

如果开启防火墙：

```bash
sudo ufw allow 8766/tcp
```

## 可选环境变量

```bash
export QUANT_WEB_PORT=8766
export QUANT_WEB_TOP=50
export QUANT_WEB_MAX_STOCKS=800
export QUANT_WEB_SCHEDULE=15:35,21:00
export QUANT_WEB_TOKEN='你的访问令牌'
```

若设置了 `QUANT_WEB_TOKEN`，网页点击“立即运行量化扫描”时会提示输入令牌。
