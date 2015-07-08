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
package org.apache.sqoop.submission.spark;

/*
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.driver.SubmissionEngine;
import org.apache.sqoop.error.code.MapreduceSubmissionError;
import org.apache.sqoop.execution.mapreduce.MRJobRequest;
import org.apache.sqoop.execution.mapreduce.MapreduceExecutionEngine;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.job.MRJobConstants;
import org.apache.sqoop.job.mr.MRConfigurationUtils;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.SubmissionError;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
*/

import java.io.Serializable;

import org.apache.sqoop.driver.SubmissionEngine;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.execution.spark.SparkExecutionEngine;
import org.apache.log4j.Logger;

//Apache Spark imports
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * This is very simple and straightforward implementation of spark based
 * submission engine.
 */
public class SparkSubmissionEngine extends SubmissionEngine implements Serializable {

  private static Logger LOG = Logger.getLogger(SparkSubmissionEngine.class);
  //private SparkConf sparkJobConf;
  //private JavaSparkContext sparkJavaContext;

  /**
   * Global configuration object that is build from hadoop configuration files
   * on engine initialization and cloned during each new submission creation.
   */
  //private Configuration globalConfiguration;


  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(MapContext context, String prefix) {
    super.initialize(context, prefix);
    LOG.info("Initializing Spark Submission Engine");

    /*
    sparkJobConf = new SparkConf().setAppName("Sqoop on Spark").setMaster("local");
    sparkJavaContext = new JavaSparkContext(sparkJobConf);
    */
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void destroy() {

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isExecutionEngineSupported(Class<?> executionEngineClass) {
    return executionEngineClass == SparkExecutionEngine.class;
    //return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean submit(JobRequest mrJobRequest) {

    //Test stub
    SparkConf sparkJobConf = new SparkConf().setAppName("Sqoop on Spark").setMaster("local");
    JavaSparkContext sparkJavaContext = new JavaSparkContext(sparkJobConf);

    String logFile = "/Users/banmeet.singh/spark-1.3.1-bin-cdh4/README.md"; // Should be some file on your system
    JavaRDD<String> logData = sparkJavaContext.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

    LOG.info("Lines with a: " + numAs + ", lines with b: " + numBs);

    return true;
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public void stop(String externalJobId) {

  }


  /**
   * {@inheritDoc}
   */
  @Override
  public void update(MSubmission submission) {

  }


}
