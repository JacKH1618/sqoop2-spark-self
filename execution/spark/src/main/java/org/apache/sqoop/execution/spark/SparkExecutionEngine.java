/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.execution.spark;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.io.NullWritable;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.MutableMapContext;
import org.apache.sqoop.driver.ExecutionEngine;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.mapredsparkcommon.MRJobConstants;
import org.apache.sqoop.job.etl.From;
import org.apache.sqoop.job.etl.To;
//import org.apache.sqoop.job.mr.SqoopInputFormat;

/**
 *
 */
public class SparkExecutionEngine extends ExecutionEngine {

  /**
   *  {@inheritDoc}
   */
  @Override
  public JobRequest createJobRequest() {
    return new SparkJobRequest();
  }

  public void prepareJob(JobRequest jobRequest) {

    SparkJobRequest sparkJobRequest = (SparkJobRequest)jobRequest;

    // Add jar dependencies
    addDependencies(sparkJobRequest);

    // Configure classes for import
    sparkJobRequest.setInputFormatClass(SqoopInputFormatSpark.class);

    sparkJobRequest.setOutputFormatClass(SqoopNullOutputFormatSpark.class);
    sparkJobRequest.setOutputKeyClass(SqoopWritableListWrapper.class);
    sparkJobRequest.setOutputValueClass(NullWritable.class);

    From from = (From) sparkJobRequest.getFrom();
    To to = (To) sparkJobRequest.getTo();
    MutableMapContext context = sparkJobRequest.getDriverContext();
    context.setString(MRJobConstants.JOB_ETL_PARTITIONER, from.getPartitioner().getName());
    context.setString(MRJobConstants.JOB_ETL_EXTRACTOR, from.getExtractor().getName());
    context.setString(MRJobConstants.JOB_ETL_LOADER, to.getLoader().getName());
    context.setString(MRJobConstants.JOB_ETL_FROM_DESTROYER, from.getDestroyer().getName());
    context.setString(MRJobConstants.JOB_ETL_TO_DESTROYER, to.getDestroyer().getName());
    context.setString(MRJobConstants.FROM_INTERMEDIATE_DATA_FORMAT,
        sparkJobRequest.getIntermediateDataFormat(Direction.FROM).getName());
    context.setString(MRJobConstants.TO_INTERMEDIATE_DATA_FORMAT,
        sparkJobRequest.getIntermediateDataFormat(Direction.TO).getName());

    if(sparkJobRequest.getExtractors() != null) {
      context.setInteger(MRJobConstants.JOB_ETL_EXTRACTOR_NUM, sparkJobRequest.getExtractors());
    }
  }



  /**
   * Our execution engine have additional dependencies that needs to be available
   * at mapreduce job time. This method will register all dependencies in the request
   * object.
   *
   * @param jobrequest Active job request object.
   */
  protected void addDependencies(SparkJobRequest jobrequest) {
    // Guava
    jobrequest.addJarForClass(ThreadFactoryBuilder.class);
  }
}
