/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.common.volume;

import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;

import java.io.IOException;

/**
 * This class is used to track Volume IO stats for each HDDS Volume.
 */
public class VolumeInfoStats {

  private String metricsSourceName = VolumeInfoStats.class.getSimpleName();
  private String VolumeRootStr;
  private HddsVolume volume;

//  private @Metric MutableCounterLong storageType;
//  private @Metric MutableCounterLong capacityUsed;
//  private @Metric MutableCounterLong capacityAvailable;
//  private @Metric MutableCounterLong capacityReserved;
//  private @Metric MutableCounterLong totalBlocks;
//  private @Metric MutableCounterLong totalCapacity;

  @Deprecated
  public VolumeInfoStats() {
    init();
  }

  /**
   * @param identifier Typically, path to volume root. e.g. /data/hdds
   */
  public VolumeInfoStats(String identifier) {
    this.metricsSourceName += '-' + identifier;
    this.VolumeRootStr = identifier;
    init();
  }

  public void init() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.register(metricsSourceName, "Volume Info Statistics", this);

    try {
      volume = new HddsVolume.Builder(VolumeRootStr).build();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(metricsSourceName);
  }

  public String getMetricsSourceName() {
    return metricsSourceName;
  }


  /**
   * Return the Storage type for the Volume
   */
  public String getStorageType() {
    return volume.getStorageType().name();
  }

  /**
   * Calculate available space use method A.
   * |----used----|   (avail)   |++++++++reserved++++++++|
   * |<-     capacity         ->|
   *|<-   ---------------    total   ---------------  ->|
   * A) avail = capacity - used
   * B) Total = capacity + reserved
   */

  /**
   * Return the Storage type for the Volume
   */
  public long getCapacityUsed() {
    return volume.getVolumeInfo().getCapacity();
  }

  /**
   * Return the Total Available capacity of the Volume.
   */
  public long getCapacityAvailable() {
    return volume.getVolumeInfo().getAvailable();
  }

  /**
   * Return the Total Reserved of the Volume.
   */
  public long getCapacityReserved() {
    return volume.getVolumeInfo().getReservedInBytes();
  }

  /**
   * Return the Total capacity of the Volume.
   */
  public long getTotalCapacity() {
    return (volume.getVolumeInfo().getCapacity()+volume.getVolumeInfo().getReservedInBytes());
  }

}
