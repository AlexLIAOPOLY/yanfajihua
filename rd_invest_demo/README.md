# 研发投入治理平台 DEMO

可直接部署到 GitHub + Render 的 FastAPI 项目，前后端同仓（后端 API + 静态前端）。

## 项目结构

```text
rd_invest_demo/
├── app/
│   ├── main.py
│   ├── routers/api.py
│   ├── services/
│   └── static/
├── data/
├── uploads/
├── tmp/
├── requirements.txt
├── run.sh
├── render.yaml
└── .env.example
```

## 本地启动

```bash
cd /Users/liaowang/Desktop/研发项目/rd_invest_demo
python3 -m pip install -r requirements.txt
cp .env.example .env
export DS_API_KEY=你的Key
uvicorn app.main:app --reload --host 0.0.0.0 --port 3210
```

打开 [http://127.0.0.1:3210](http://127.0.0.1:3210)

## GitHub 上传前说明

1. 不要提交真实 API Key。
2. `.gitignore` 已忽略 `.env`、`data/*.db`、`uploads/*` 运行产物。
3. 前端已移除硬编码 DS Key，默认走服务端环境变量（Render 填写）。

## Render 部署（Blueprint）

仓库根目录已提供 `render.yaml`，可直接在 Render 用 Blueprint 导入。

### Render 页面怎么填写

1. Render 首页点击 `New +` -> `Blueprint`
2. 选择你的 GitHub 仓库
3. Blueprint 文件路径：`render.yaml`（默认自动识别）
4. 确认服务参数
   - Runtime: `Python`
   - Build Command: `pip install -r requirements.txt`
   - Start Command: `bash run.sh`
5. 环境变量（Environment）
   - `DS_API_KEY` = 你的 DeepSeek Key（建议必填）
   - `OPENAI_API_KEY` = 你的 OpenAI Key（可选）
   - `DEFAULT_LLM_PROVIDER` = `deepseek`
   - `DEFAULT_DEEPSEEK_MODEL` = `deepseek-chat`
   - `DEFAULT_OPENAI_MODEL` = `gpt-4o-mini`
   - `DEFAULT_REPORT_MONTH` = `2025-11`
   - `DEFAULT_REPORT_YEAR` = `2025`
6. 点击 `Apply` / `Create` 部署

部署完成后访问 Render 分配的域名即可。

## 生产环境建议

- 如果你需要持久化数据库，不要使用 Render 临时文件系统，改为外部托管数据库。
- 当前 SQLite 适合 DEMO 与小规模测试。
