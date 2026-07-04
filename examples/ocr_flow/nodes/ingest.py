"""ingest 节点:生成一张含中文的测试图片,模拟待识别扫描件。

PIL 默认字体不支持中文,尝试常见 CJK 字体路径,找不到则回退默认字体。
图片写入框架注入的 output_dir,artifact 登记绝对路径供 view 展示与下游读取。
"""

from esflow import Node

_CJK_FONTS = [
    "/System/Library/Fonts/PingFang.ttc",            # macOS
    "/System/Library/Fonts/STHeiti Light.ttc",       # macOS 旧
    "/Library/Fonts/Arial Unicode.ttf",              # macOS 兜底
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",  # Linux Noto
    "/usr/share/fonts/truetype/wqy/wqy-zenhei.ttc",  # Linux 文泉驿
    "C:/Windows/Fonts/msyh.ttc",                     # Windows 微软雅黑
]


def _cjk_font(size: int = 32):
    from PIL import ImageFont

    for path in _CJK_FONTS:
        try:
            return ImageFont.truetype(path, size)
        except OSError:
            continue
    return ImageFont.load_default()


class Ingest(Node):
    id = "ingest"
    title = "生成测试图片"

    def run(self, ctx) -> dict:
        from PIL import Image, ImageDraw

        img = Image.new("RGB", (640, 120), "white")
        draw = ImageDraw.Draw(img)
        draw.text((40, 40), "这是一个ocr场景的flow测试", fill="black", font=_cjk_font())
        path = str(self.output_dir / "input.png")
        img.save(path)
        return {"image_path": path, "size": img.size}
