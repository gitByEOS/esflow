"""preprocess 节点:放大 2x(LANCZOS)+ 锐化,提升小字识别率。

Pillow 在 run 内 import,确保预检先于依赖加载执行。
产物写入 output_dir,artifact 登记绝对路径供 view 展示与下游读取。
"""

from easyflow import Node


class Preprocess(Node):
    id = "preprocess"
    title = "放大 + 锐化"

    def run(self, ctx) -> dict:
        from PIL import Image, ImageFilter

        upstream = ctx.get("ingest")
        path = upstream["image_path"]
        with Image.open(path) as img:
            w, h = img.size
            scaled = img.resize((w * 2, h * 2), Image.LANCZOS)
            sharpened = scaled.filter(ImageFilter.SHARPEN)
            out = str(self.output_dir / "preprocessed.png")
            sharpened.save(out)
        return {"image_path": out, "size": sharpened.size}
