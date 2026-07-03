# 事件协议

Runner 产出的唯一事件流(`WorkflowJobEvent`):

| type         | 含义                                                |
| ------------ | ------------------------------------------------- |
| `trace`      | 节点状态变更(queued / running / done / error / skipped) |
| `delta`      | 节点产出增量文本                                          |
| `checkpoint` | 节点到暂停点,等外部 continue / retry / abort                |
| `final`      | 节点最终 artifact                                     |
| `error`      | 错误(含接手/脱手确认失败)                                    |
| `end`        | job 结束                                            |

事件经 `apply_event` 折叠成内存 `JobState` 供视图消费。
