package de.samply.directory_sync_service;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

public class Util {

  public static <K, V> Map<K, V> mapOf() {
    return new HashMap<>();
  }

    /**
     * Creates a new map with a single entry consisting of the specified key and value.
     *
     * @param key the key for the entry
     * @param value the value associated with the key
     * @return a new map containing the specified key-value pair
     */
  public static <K, V> Map<K, V> mapOf(K key, V value) {
    HashMap<K, V> map = new HashMap<>();
    map.put(key, value);
    return map;
  }

  /**
  * Get a printable stack trace from an Exception object.
  * @param e
  * @return
 */
  public static String traceFromException(Exception e) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
       return sw.toString();
   }
}
