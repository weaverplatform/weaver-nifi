package com.weaverplatform.nifi.util;

import java.util.HashMap;
import java.util.Map;

public class LockRegistry {

  private static final Map<String, Boolean> statusRegistry = new HashMap();
  private static final Map<String, Lock> lockRegistry = new HashMap();

  public static synchronized boolean request(String action, String objectHash) throws InterruptedException {

    String key = action + objectHash;

    if(!lockRegistry.containsKey(key)) {
      lockRegistry.put(key, new Lock());
    }
    lockRegistry.get(key).lock();

    if(!statusRegistry.containsKey(key)) {
      statusRegistry.put(key, false);
    }
    return statusRegistry.get(key);
  }

  public static synchronized void release(String action, String objectHash) {

    String key = action + objectHash;

    statusRegistry.put(key, true);
    lockRegistry.get(key).unlock();
    lockRegistry.remove(key);
  }
}
