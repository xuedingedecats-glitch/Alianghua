# A股量化推荐与次日开盘监控网站

本项目对沪深 A 股主板、创业板、科创板做全市场基础过滤和日线战法深筛，展示规则评分、战法分类、风险区间、基本面摘要、历史观察反馈，并支持把候选加入次日开盘监控。

> 规则评分只是排序分，不是上涨概率；历史“观察期正收益比例”以信号日收盘为观察基准，不等同于真实成交胜率或收益。系统不会自动下单。

## 主要功能

- 工作日收盘后按 `15:35,21:00` 自动扫描，也可使用管理令牌手动补跑；
- 18 种战法，按趋势动量、突破、回踩低吸、K线形态、涨停情绪、超跌反转分类；
- 首页候选单选/多选/全选加入次日早盘监控，并支持任意六位沪深 A 股代码；
- 次日 `09:35,09:50,10:15,10:30` 自动核验，只有 `09:35-11:30` 早盘窗口允许生成新的确认结论，下午仅复盘；
- 早盘页提供账户资金、单笔风险、单股仓位、组合总仓位和组合总止损风险约束，按候选排序自动折算整手组合预案；
- 保存并展示最近开盘核验轨迹，可导出当前核验 CSV（已防范表格公式注入）；
- 交易复盘台：手动记录实际/模拟建仓、初始止损、移动保护价和退出结果，自动计算成本、风险、盈亏、收益率与 R 倍数；
- 早盘核验结果可一键带入交易复盘表单，形成“推荐—建仓—风控—退出—复盘”闭环；
- 个股基本面摘要与公开行情双数据源降级；
- 自动同步开关：只同步达到最低规则分且风险标签合格的候选；
- 数据质量闸门：行情日期不一致、非当日交易数据、K线失败率过高时不会覆盖旧监控池；
- 管理令牌仅保存在浏览器当前标签页的 `sessionStorage`，关闭标签页后失效；
- 请求体限制、接口限速、并发上限、开盘行情单飞刷新、安全响应头和 systemd 沙箱；
- 未配置管理令牌时禁止监听公网地址，避免误部署为无鉴权管理服务。

## 本地启动与测试

```powershell
cd C:\Users\baoyu\Documents\Codex\2026-07-11\bia\outputs\a_share_quant_web
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python app.py --host 127.0.0.1 --port 8766
```

访问 `http://127.0.0.1:8766/`。

## Linux 部署

推荐使用仓库内脚本：

```powershell
.\deploy_to_server.ps1 -HostName 服务器IP -User root
```

脚本会上传源码至 `/opt/a_share_quant_web`，安装虚拟环境、创建低权限账户 `quantweb`，把运行数据放在 `/var/lib/quant-web`，并启动 `quant-web.service`。

服务端关键命令：

```bash
systemctl status quant-web --no-pager
journalctl -u quant-web -n 100 --no-pager
systemctl restart quant-web
```

## 关键环境变量

配置文件默认是 `/etc/quant-web.env`：

```bash
QUANT_WEB_HOST=0.0.0.0
QUANT_WEB_PORT=8766
QUANT_WEB_TOP=80
QUANT_WEB_FULL=1
QUANT_WEB_WORKERS=8
QUANT_WEB_SCHEDULE=15:35,21:00
QUANT_WEB_OPENING_SCHEDULE=09:35,09:50,10:15,10:30
QUANT_WEB_DATA_DIR=/var/lib/quant-web
QUANT_WEB_AUTO_SYNC_MIN_SCORE=72
QUANT_WEB_AUTO_SYNC_MAX_FAILURE_RATE=0.10
QUANT_WEB_MAX_CONCURRENT_REQUESTS=64
QUANT_WEB_TOKEN=至少32位随机令牌
```

## 安全说明

- 不要把 `/etc/quant-web.env`、管理令牌、`opening_watchlist.json`、`trade_journal.json`、报告、缓存、日志或虚拟环境提交到 GitHub；
- 当前若使用公网 HTTP，管理令牌仍可能在传输途中被窃取。生产环境应通过 Nginx Proxy Manager 配置域名和 HTTPS，或仅允许可信内网/VPN访问管理接口；
- 内置限速适用于单进程实例，多实例部署应改用 Nginx/Redis 统一限速；
- 数据源属于公开行情接口，接口异常时系统会降级或阻止自动监控池更新，但仍应人工复核。
