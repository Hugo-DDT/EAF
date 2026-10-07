package io.eaf.observability.api;

public interface TraceRecorder {
    void record(TraceObservation observation);
}
// 本文件负责实现 EAF 的 TraceRecorder.java 相关代码。
