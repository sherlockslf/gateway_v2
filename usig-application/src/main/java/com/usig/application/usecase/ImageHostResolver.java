package com.usig.application.usecase;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

@FunctionalInterface
interface ImageHostResolver {

  List<InetAddress> resolve(String host) throws UnknownHostException;
}
