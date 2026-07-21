package com.monitor.function;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 基线统计 — 用于偏离检测。
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BaselineStats {
    private double mean;
    private double stddev;
    private int count;
}
