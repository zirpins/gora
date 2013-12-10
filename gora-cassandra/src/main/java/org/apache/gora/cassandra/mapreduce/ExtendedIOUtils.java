/*
 * ExtendedIOUtils.java
 *
 * created at 06.11.2013 by c.zirpins <YOURMAILADDRESS>
 *
 * Copyright (c) SEEBURGER AG, Germany. All Rights Reserved.
 */
package org.apache.gora.cassandra.mapreduce;

import java.io.IOException;

import org.apache.gora.util.ClassLoadingUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;

public class ExtendedIOUtils {
  /**
   * Stores the given object array in the configuration under the given dataKey
   *
   * @param arr
   *          the object array to store
   * @param conf
   *          the configuration to store the object into
   * @param dataKey
   *          the key to store the data
   */
  public static <T> void storeArrayToConf(T[] arr, Configuration conf, String dataKey) throws IOException {
    String classKey = dataKey + "._class";
    conf.set(classKey, arr[0].getClass().getName());
    DefaultStringifier.storeArray(conf, arr, dataKey);
  }

  /**
   * Loads the object array stored by {@link #storeArrayToConf(Object[], Configuration, String)}
   * method from the configuration under the given dataKey.
   *
   * @param conf
   *          the configuration to read from
   * @param dataKey
   *          the key to get the data from
   * @return the stored object array
   */
  @SuppressWarnings("unchecked")
  public static <T> T[] loadArrayFromConf(Configuration conf, String dataKey) throws IOException {
    String classKey = dataKey + "._class";
    String className = conf.get(classKey);
    try {
      T[] arr = (T[]) DefaultStringifier.loadArray(conf, dataKey, ClassLoadingUtils.loadClass(className));
      return arr;
    } catch (Exception ex) {
      throw new IOException(ex);
    }
  }
}
