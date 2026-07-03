"""ocr 节点:POST 预处理后的图片到 OCR 服务,取回文本。

服务协议(JSON):
    POST {OCR_BASE}/api/ocr
    body: {"image": "data:image/png;base64,<base64>"}
    resp: {"text": "..."}
OCR_BASE 从环境变量读,默认 http://localhost:11434(ollama 默认端口)。
"""

import os

from easyflow import Node


class Ocr(Node):
    id = "ocr"
    title = "OCR 识别"

    def run(self, ctx) -> dict:
        import base64
        import json
        import urllib.request

        upstream = ctx.get("preprocess")
        path = upstream["image_path"]
        with open(path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()

        url = os.environ.get("OCR_BASE", "http://localhost:11434") + "/api/ocr"
        payload = json.dumps({"image": f"data:image/png;base64,{b64}"}).encode()
        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            text = json.loads(resp.read().decode("utf-8"))["text"]
        return {"text": text.strip()}
