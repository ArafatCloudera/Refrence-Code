/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.s3.metrics;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * This class maintains S3 Gateway related metrics.
 */
@InterfaceAudience.Private
@Metrics(about = "S3 Gateway Metrics", context = OzoneConsts.OZONE)
public final class S3GatewayMetrics implements MetricsSource {

  public static final String SOURCE_NAME =
      S3GatewayMetrics.class.getSimpleName();

  private MetricsRegistry registry;
  private static S3GatewayMetrics instance;

  // BucketEndpoint Operations 
  private @Metric MutableCounterLong getBucketOpsSuccess;
  private @Metric MutableCounterLong getBucketOpsFailed;
  private @Metric MutableCounterLong bucketCreateOpsSuccess;
  private @Metric MutableCounterLong bucketCreateOpsFailed;
  private @Metric MutableCounterLong headBucketOpsSuccess;
  private @Metric MutableCounterLong deleteBucketOpsSuccess;
  private @Metric MutableCounterLong deleteBucketOpsFailed;
  private @Metric MutableCounterLong getAclSuccess;
  private @Metric MutableCounterLong getAclFailed;
  private @Metric MutableCounterLong putAclSuccess;
  private @Metric MutableCounterLong putAclFailed;
  private @Metric MutableCounterLong listMultipartUploadsOpsSuccess;
  private @Metric MutableCounterLong multipartUploadsOpsFailure;

  // RootEndpoint
  private @Metric MutableCounterLong listS3BucketsSuccess;
  private @Metric MutableCounterLong listS3BucketsFailure;

  // ObjectEndpoint
  private @Metric MutableCounterLong createMultipartKeySuccess;
  private @Metric MutableCounterLong createMultipartKeyFailure;
  private @Metric MutableCounterLong copyObjectSuccess;
  private @Metric MutableCounterLong copyObjectFailure;
  private @Metric MutableCounterLong createKeySuccess;
  private @Metric MutableCounterLong createKeyFailure;
  private @Metric MutableCounterLong listPartsSuccess;
  private @Metric MutableCounterLong listPartsFailure;
  private @Metric MutableCounterLong getKeySuccess;
  private @Metric MutableCounterLong getKeyFailure;
  private @Metric MutableCounterLong headKeySuccess;
  private @Metric MutableCounterLong headKeyFailure;
  private @Metric MutableCounterLong initMultiPartUploadSuccess;
  private @Metric MutableCounterLong initMultiPartUploadFailure;
  private @Metric MutableCounterLong completeMultiPartUploadSuccess;
  private @Metric MutableCounterLong completeMultiPartUploadFailure;
  private @Metric MutableCounterLong abortMultiPartUploadSuccess;
  private @Metric MutableCounterLong abortMultiPartUploadFailure;
  private @Metric MutableCounterLong deleteKeySuccess;
  private @Metric MutableCounterLong deleteKeyFailure;


  /**
   * Private constructor.
   */
  private S3GatewayMetrics() {
    this.registry = new MetricsRegistry(SOURCE_NAME);
  }

  /**
   * Create and returns S3 Gateway Metrics instance.
   *
   * @return S3GatewayMetrics
   */
  public static synchronized S3GatewayMetrics create() {
    if (instance != null) {
      return instance;
    }
    MetricsSystem ms = DefaultMetricsSystem.instance();
    instance = ms.register(SOURCE_NAME, "S3 Gateway Metrics",
        new S3GatewayMetrics());
    return instance;
  }

  /**
   * Unregister the metrics instance.
   */
  public static void unRegister() {
    instance = null;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);

    // BucketEndpoint
    getBucketSuccess.snapshot(recordBuilder, true);
    getBucketFailure.snapshot(recordBuilder, true);
    createBucketSuccess.snapshot(recordBuilder, true);
    createBucketFailure.snapshot(recordBuilder, true);
    headBucketSuccess.snapshot(recordBuilder, true);
    deleteBucketSuccess.snapshot(recordBuilder, true);
    deleteBucketFailure.snapshot(recordBuilder, true);
    getAclSuccess.snapshot(recordBuilder, true);
    getAclFailure.snapshot(recordBuilder, true);
    putAclSuccess.snapshot(recordBuilder, true);
    putAclFailure.snapshot(recordBuilder, true);
    listMultipartUploadsSuccess.snapshot(recordBuilder, true);
    listMultipartUploadsFailure.snapshot(recordBuilder, true);
  }

  // INC
  public void incGetBucketSuccess() {
    getBucketSuccess.incr();
  }

  public void incGetBucketFailure() {
    getBucketFailure.incr();
  }

  public void incListS3BucketsSuccess() {
    listS3BucketsSuccess.incr();
  }


  public void incListS3BucketsFailure() {
    listS3BucketsFailure.incr();
  }


  public void incCreateBucketSuccess() {
    createBucketSuccess.incr();
  }

  public void incCreateBucketFailure() {
    createBucketFailure.incr();
  }

  public void incPutAclSuccess() {
    putAclSuccess.incr();
  }

  public void incPutAclFailure() {
    putAclFailure.incr();
  }

  public void incGetAclSuccess() {
    getAclSuccess.incr();
  }

  public void incGetAclFailure() {
    getAclFailure.incr();
  }

  public void incHeadBucketSuccess() {
    headBucketSuccess.incr();
  }


  public void incDeleteBucketSuccess() {
    deleteBucketSuccess.incr();
  }

  public void incDeleteBucketFailure() {
    deleteBucketFailure.incr();
  }

  public void incHeadKeySuccess() {
    headKeySuccess.incr();
  }

  public void incHeadKeyFailure() {
    headKeyFailure.incr();
  }

  // GET
  public long getListS3BucketsSuccess() {
    return listS3BucketsSuccess.value();
  }

  public long getHeadBucketSuccess() {
    return headBucketSuccess.value();
  }

  public long getHeadKeySuccess() {
    return headKeySuccess.value();
  }

  public long getGetBucketSuccess() {
    return getBucketSuccess.value();
  }

  public long getGetBucketFailure() {
    return getBucketFailure.value();
  }

  public long getCreateBucketSuccess(){
    return createBucketSuccess.value();
  }

  public long getCreateBucketFailure(){
    return createBucketFailure.value();
  }

  public long getDeleteBucketSuccess(){
    return deleteBucketSuccess.value();
  }

  public long getDeleteBucketFaliure(){
    return deleteBucketFailure.value();
  }

  public long getGetAclSuccess(){
    return getAclSuccess.value();
  }

  public long getGetAclFaliure(){
    return getAclFailure.value();
  }
}
