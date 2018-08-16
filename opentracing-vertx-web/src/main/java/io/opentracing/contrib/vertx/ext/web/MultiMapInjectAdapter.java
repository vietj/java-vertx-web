package io.opentracing.contrib.vertx.ext.web;

import io.opentracing.propagation.TextMap;
import io.vertx.core.MultiMap;
import java.util.Iterator;
import java.util.Map.Entry;

/**
 * @author Pavol Loffay
 */
public class MultiMapInjectAdapter implements TextMap {

  private final MultiMap multiMap;

  public MultiMapInjectAdapter(MultiMap multiMap) {
    this.multiMap = multiMap;
  }

  @Override
  public Iterator<Entry<String, String>> iterator() {
    return multiMap.iterator();
  }

  @Override
  public void put(String key, String value) {
    multiMap.add(key, value);
  }
}
