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

import java.io.*;
import java.io.File;
import java.io.FilenameFilter;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.net.MalformedURLException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.matcher.Matcher;
import org.apache.sqoop.connector.matcher.MatcherFactory;
import org.apache.sqoop.driver.SubmissionEngine;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.error.code.MRExecutionError;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.execution.spark.SqoopInputFormatSpark;
import org.apache.sqoop.execution.spark.SqoopWritableListWrapper;
import org.apache.sqoop.job.MRJobConstants;
import org.apache.sqoop.job.PrefixContext;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.job.mr.MRConfigurationUtils;
import org.apache.sqoop.job.mr.SqoopSplit;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.execution.spark.SparkExecutionEngine;
//import org.apache.sqoop.execution.spark.SqoopInputFormatSpark;
import org.apache.sqoop.execution.spark.SparkJobRequest;
import org.apache.log4j.Logger;
import org.apache.sqoop.error.code.SparkSubmissionError;
import org.apache.sqoop.common.SqoopException;
//import org.apache.sqoop.job.mr.SqoopInputFormat;

//Hadoop imports
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapreduce.InputFormat;

//Apache Spark imports
//import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.sqoop.model.SubmissionError;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.submission.SubmissionStatus;
import org.apache.sqoop.utils.ClassUtils;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.rdd.HadoopRDD;

/**
 * This is very simple and straightforward implementation of spark based
 * submission engine.
 */
public class SparkSubmissionEngine extends SubmissionEngine {

  private static Logger LOG = Logger.getLogger(SparkSubmissionEngine.class);
  private SparkConf sparkConf;
  private JavaSparkContext sc;

  /**
   * Global configuration object that is built from hadoop configuration files
   * on engine initialization and cloned during each new submission creation.
   */
  private Configuration globalConfiguration;
  //private transient JobConf jobConf;

  /**
   * {@inheritDoc}
   */
  @Override
  public void initialize(MapContext context, String prefix) {
    super.initialize(context, prefix);
    LOG.info("Initializing Spark Submission Engine");

    // Build global configuration, start with empty configuration object
    globalConfiguration = new Configuration();
    globalConfiguration.clear();

    // Load configured hadoop configuration directory
    String configDirectory = context.getString(prefix + Constants.CONF_CONFIG_DIR);

    // Git list of files ending with "-site.xml" (configuration files)
    File dir = new File(configDirectory);
    String [] files = dir.list(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("-site.xml");
      }
    });

    if(files == null) {
      throw new SqoopException(SparkSubmissionError.SPARK_0002,
          "Invalid Hadoop configuration directory (not a directory or permission issues): " + configDirectory);
    }

    // Add each such file to our global configuration object
    for (String file : files) {
      LOG.info("Found hadoop configuration file " + file);
      try {
        globalConfiguration.addResource(new File(configDirectory, file).toURI().toURL());
      } catch (MalformedURLException e) {
        LOG.error("Can't load configuration file: " + file, e);
      }
    }

    // Save our own property inside the job to easily identify Sqoop jobs
    globalConfiguration.setBoolean(Constants.SQOOP_JOB, true);

    // Initialize the Spark Context
    sparkConf = new SparkConf().setAppName("Sqoop on Spark").setMaster("local");
    sc = new JavaSparkContext(sparkConf);

  }


  /**
   * {@inheritDoc}
   */
  @Override
  public void destroy() {
    super.destroy();
    LOG.info("Destroying Spark Submission Engine");
    sc.stop();
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
  public boolean submit(JobRequest sparkJobRequest) {

    //This additional setting up of configuration is to be done on each submission
    //(as in the MR engine)
    SparkJobRequest request = (SparkJobRequest) sparkJobRequest;

    // Clone global configuration
    //jackh: Check 'final' - probably added by Intellij while refactoring conf (from run() in map()) to configuration
    final Configuration configuration = new Configuration(globalConfiguration);

    // Serialize driver context into job configuration
    for(Map.Entry<String, String> entry: request.getDriverContext()) {
      if (entry.getValue() == null) {
        LOG.warn("Ignoring null driver context value for key " + entry.getKey());
        continue;
      }
      configuration.set(entry.getKey(), entry.getValue());
    }

    // Serialize connector context as a sub namespace
    for(Map.Entry<String, String> entry : request.getConnectorContext(Direction.FROM)) {
      if (entry.getValue() == null) {
        LOG.warn("Ignoring null connector context value for key " + entry.getKey());
        continue;
      }
      configuration.set(
          MRJobConstants.PREFIX_CONNECTOR_FROM_CONTEXT + entry.getKey(),
          entry.getValue());
    }

    for(Map.Entry<String, String> entry : request.getConnectorContext(Direction.TO)) {
      if (entry.getValue() == null) {
        LOG.warn("Ignoring null connector context value for key " + entry.getKey());
        continue;
      }
      configuration.set(
          MRJobConstants.PREFIX_CONNECTOR_TO_CONTEXT + entry.getKey(),
          entry.getValue());
    }

    // Promote all required jars to the job
    configuration.set("tmpjars", StringUtils.join(request.getJars(), ","));

    try {
      Job job = new Job(configuration);

      // Adding link, job and connector schema configurations to the Mapreduce configuration object instead of the
      // Hadoop credentials cache. This is because hadoop, for security reasons, does not serialize the credentials
      // cache for sending over the wire (only the Configuration object is serialized, while the credentials cache
      // resides in the JobConf object).
      // Adding this configuration information to the Configuration object and sending over the wire is a security
      // issue that must be addressed later.

      // from and to link configs
      MRConfigurationUtils.setConnectorLinkConfigUnsafe(Direction.FROM, job.getConfiguration(), request.getConnectorLinkConfig(Direction.FROM));
      MRConfigurationUtils.setConnectorLinkConfigUnsafe(Direction.TO, job.getConfiguration(), request.getConnectorLinkConfig(Direction.TO));

      // from and to job configs
      MRConfigurationUtils.setConnectorJobConfigUnsafe(Direction.FROM, job.getConfiguration(), request.getJobConfig(Direction.FROM));
      MRConfigurationUtils.setConnectorJobConfigUnsafe(Direction.TO, job.getConfiguration(), request.getJobConfig(Direction.TO));

      // driver config
      MRConfigurationUtils.setDriverConfig(job, request.getDriverConfig());

      // from and to connector configs
      MRConfigurationUtils.setConnectorSchemaUnsafe(Direction.FROM, job.getConfiguration(), request.getJobSubmission().getFromSchema());
      MRConfigurationUtils.setConnectorSchemaUnsafe(Direction.TO, job.getConfiguration(), request.getJobSubmission().getToSchema());

      // Retaining to minimize change to existing functioning code
      MRConfigurationUtils.setConnectorLinkConfig(Direction.FROM, job, request.getConnectorLinkConfig(Direction.FROM));
      MRConfigurationUtils.setConnectorLinkConfig(Direction.TO, job, request.getConnectorLinkConfig(Direction.TO));
      MRConfigurationUtils.setConnectorJobConfig(Direction.FROM, job, request.getJobConfig(Direction.FROM));
      MRConfigurationUtils.setConnectorJobConfig(Direction.TO, job, request.getJobConfig(Direction.TO));
      MRConfigurationUtils.setConnectorSchema(Direction.FROM, job, request.getJobSubmission().getFromSchema());
      MRConfigurationUtils.setConnectorSchema(Direction.TO, job, request.getJobSubmission().getToSchema());

      if (request.getJobName() != null) {
        job.setJobName("Sqoop: " + request.getJobName());
      } else {
        job.setJobName("Sqoop job with id: " + request.getJobId());
      }

      job.setInputFormatClass(request.getInputFormatClass());

      job.setMapperClass(request.getMapperClass());
      job.setMapOutputKeyClass(request.getMapOutputKeyClass());
      job.setMapOutputValueClass(request.getMapOutputValueClass());

      job.setOutputFormatClass(request.getOutputFormatClass());
      job.setOutputKeyClass(request.getOutputKeyClass());
      job.setOutputValueClass(request.getOutputValueClass());

      // Form the initial RDD from the Hadoop configuration object set up above
      JavaPairRDD<SqoopSplit, SqoopSplit> initRDD = sc.newAPIHadoopRDD(job.getConfiguration(),
          SqoopInputFormatSpark.class, SqoopSplit.class, SqoopSplit.class);

      // Create SparkMapTrigger object and use it to trigger mapToPair()
      ConfigurationWrapper wrappedConf = new ConfigurationWrapper(job.getConfiguration());
      SparkMapTrigger sparkMapTriggerObj = new SparkMapTrigger(initRDD, wrappedConf);
      JavaPairRDD<SqoopWritableListWrapper, NullWritable> mappedRDD = sparkMapTriggerObj.triggerSparkMapValues();

      // Add reduce phase/any transformation code here

      // Calls the OutputFormat for writing
      mappedRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());

      // Data transfer completed successfully if here
      request.getJobSubmission().setStatus(SubmissionStatus.SUCCEEDED);

      return true;

    } catch (Exception e) {
      SubmissionError error = new SubmissionError();
      error.setErrorSummary(e.toString());
      StringWriter writer = new StringWriter();
      e.printStackTrace(new PrintWriter(writer));
      writer.flush();
      error.setErrorDetails(writer.toString());

      request.getJobSubmission().setError(error);
      LOG.error("Error in submitting job", e);
      return false;
    }

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
