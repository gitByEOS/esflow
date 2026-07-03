"""skip_flow:两组 serial fallback 链——多源兜底抓取 + 解析格式降级。

链路:
    trigger
      → fetch_from_ssr / fetch_from_wechat / fetch_from_bili   (serial, 多源兜底抓取)
        → merge
          → parse_to_html / parse_to_md                       (serial, 解析降级)
            → done

serial 语义:同轮就绪节点中属于 serial 集合的,只启动 nodes 声明顺序最靠前的,
其他等下一轮。前一个完成(或 skip)后,后一个才就绪启动,accept 检查前一个产物
决定 skip 还是接手兜底。

跑:
    easyflow run examples/skip_flow
    easyflow view examples/skip_flow
"""

from easyflow import flow, edge


@flow(id="skip_flow", title="多源兜底抓取 + 解析降级")
class SkipFlow:
    nodes = [
        "trigger",
        "fetch_from_ssr", "fetch_from_wechat", "fetch_from_bili",
        "merge",
        "parse_to_html", "parse_to_md",
        "done",
    ]
    edges = [
        edge("trigger", "fetch_from_ssr"),
        edge("trigger", "fetch_from_wechat"),
        edge("trigger", "fetch_from_bili"),
        edge("fetch_from_ssr", "merge"),
        edge("fetch_from_wechat", "merge"),
        edge("fetch_from_bili", "merge"),
        edge("merge", "parse_to_html"),
        edge("merge", "parse_to_md"),
        edge("parse_to_html", "done"),
        edge("parse_to_md", "done"),
    ]
    serial = {
        "fetch_from_ssr", "fetch_from_wechat", "fetch_from_bili",
        "parse_to_html", "parse_to_md",
    }
