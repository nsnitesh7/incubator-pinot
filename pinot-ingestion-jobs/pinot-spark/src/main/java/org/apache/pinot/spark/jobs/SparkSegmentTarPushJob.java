/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.spark.jobs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.ingestion.common.ControllerRestApi;
import org.apache.pinot.ingestion.common.JobConfigConstants;
import org.apache.pinot.ingestion.jobs.SegmentTarPushJob;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;


public class SparkSegmentTarPushJob extends SegmentTarPushJob {
  private final boolean _enableParallelPush;

  public SparkSegmentTarPushJob(Properties properties) {
    super(properties);
    _enableParallelPush =
        Boolean.parseBoolean(properties.getProperty(JobConfigConstants.ENABLE_PARALLEL_PUSH, "false"));
  }

  @Override
  public void run()
      throws Exception {
    if (!_enableParallelPush) {
      super.run();
    } else {
      FileSystem fileSystem = FileSystem.get(new Path(_segmentPattern).toUri(), getConf());
      List<Path> segmentPathsToPush = getDataFilePaths(_segmentPattern);
      List<String> segmentsToPush = new ArrayList<>();
      segmentPathsToPush.forEach(path -> {
        segmentsToPush.add(path.toString());
      });
      JavaSparkContext sparkContext = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate());
      JavaRDD<String> pathRDD = sparkContext.parallelize(segmentsToPush, segmentsToPush.size());
      pathRDD.foreach(segmentTarPath -> {
        try (ControllerRestApi controllerRestApi = getControllerRestApi()) {
          // TODO: Deal with invalid prefixes in the future
          List<String> currentSegments = controllerRestApi.getAllSegments("OFFLINE");
          controllerRestApi.pushSegments(fileSystem, Arrays.asList(new Path(segmentTarPath)));
          if (_deleteExtraSegments) {
            controllerRestApi
                .deleteSegmentUris(getSegmentsToDelete(currentSegments, Arrays.asList(new Path(segmentTarPath))));
          }
        }
      });
    }
  }
}
