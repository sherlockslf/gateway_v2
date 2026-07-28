package com.usig.application.usecase;

import java.net.URI;

@FunctionalInterface
interface ImageUrlProbe {

  ImageUrlProbeResult probe(URI uri);
}
