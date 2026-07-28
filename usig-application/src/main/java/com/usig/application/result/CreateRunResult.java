package com.usig.application.result;

import com.usig.domain.run.Run;

public record CreateRunResult(Run run, String eventsUrl) {}
