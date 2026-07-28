package com.usig.application.memory;

import com.usig.application.port.DeliveryMetric;
import com.usig.application.port.DeliveryMetricsRecorder;
import java.time.Duration;

public final class NoopDeliveryMetricsRecorder implements DeliveryMetricsRecorder {

  @Override
  public void eventOut(DeliveryMetric metric) {}

  @Override
  public void streamAddedLatency(DeliveryMetric metric, Duration latency) {}
}
