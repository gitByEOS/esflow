"""ocr_flow:图片 OCR 流程,4 节点线性 DAG。

链路:
    ingest → preprocess → ocr → export

    ingest:     生成待识别测试图片(小字,模拟扫描件)
    preprocess: 放大 2x + 锐化,提升小字 OCR 识别率(用 Pillow)
    ocr:        POST 图片到本地 OCR 服务(localhost:8080),取回文本
    export:     识别文本落盘

预检(run.py 启动时执行,任一失败则不启动 runner):
    - check_ocr_service: OCR 服务 /health 是否返回 healthy(地址由 OCR_BASE 指定,默认 http://localhost:11434)
    - check_image_lib:   Pillow 是否可用(preprocess 依赖)

节点内依赖库(Pillow)在 run 方法内 import,让预检先于依赖加载执行;
否则 loader 加载 nodes/*.py 时就会因 ImportError 失败,预检无从触发。

跑:
    python examples/ocr_flow/run.py        # 含预检的完整 skill 入口
    easyflow run examples/ocr_flow         # 跳过预检直接跑(CLI 调试用)
    easyflow view examples/ocr_flow        # Web 调试界面
"""

from easyflow import flow, edge


@flow(id="ocr_flow", title="图片 OCR 流程")
class OcrFlow:
    nodes = ["ingest", "preprocess", "ocr", "export"]
    edges = [
        edge("ingest", "preprocess"),
        edge("preprocess", "ocr"),
        edge("ocr", "export"),
    ]
