package com.usig.application.port;

import java.time.Duration;

public interface DeliveryMetricsRecorder {

  void eventOut(DeliveryMetric metric);

  void streamAddedLatency(DeliveryMetric metric, Duration latency);
}
