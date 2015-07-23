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
package org.apache.sqoop.submission.spark.engine;

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
import java.io.IOException;
import java.io.Serializable;
import java.lang.Integer;
import java.net.MalformedURLException;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.driver.SubmissionEngine;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.execution.mapreduce.MRJobRequest;
import org.apache.sqoop.job.MRJobConstants;
import org.apache.sqoop.job.mr.MRConfigurationUtils;
import org.apache.sqoop.job.mr.SqoopInputFormat;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.execution.spark.SparkExecutionEngine;
//import org.apache.sqoop.execution.spark.SqoopInputFormatSpark;
import org.apache.sqoop.execution.spark.SparkJobRequest;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.error.code.SparkSubmissionError;
import org.apache.sqoop.common.SqoopException;
//import org.apache.sqoop.job.mr.SqoopInputFormat;
import org.apache.sqoop.job.mr.SqoopSplit;

//Hadoop imports
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.NullWritable;
//import org.apache.hadoop.mapreduce.InputFormat;

//Apache Spark imports
//import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
//import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.sqoop.model.SubmissionError;
import scala.Tuple2;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.rdd.HadoopRDD;

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
  private transient Configuration globalConfiguration;
  //private transient JobConf jobConf;

  //private static final java.io.ObjectStreamField[] serialPersistentFields =  {
  //new ObjectStreamField("globalConfiguration", org.apache.hadoop.conf.Configuration.class) };

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

    // Create jobConf object to be used for creating the RDD from InputFormat
    //try {
    //jobConf = new JobConf(globalConfiguration);
    //jobClient = new JobClient(new JobConf(globalConfiguration));
    //} catch (IOException e) {
    //throw new SqoopException(SparkSubmissionError.SPARK_0002, e);
    //}

    //if(isLocal()) {
    //LOG.info("Detected MapReduce local mode, some methods might not work correctly.");
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
  public boolean submit(JobRequest sparkJobRequest) {

    //Move this to initialize()
    SparkConf sparkConf = new SparkConf().setAppName("Sqoop on Spark").setMaster("local")/*.set("spark.authenticate", "true"). set("spark.authenticate.secret", "jackh")*/;
    JavaSparkContext sc = new JavaSparkContext(sparkConf);

    //This additional setting up of configuration is to be done on each submission
    //(as in the MR engine)
    SparkJobRequest request = (SparkJobRequest) sparkJobRequest;

    // Clone global configuration
    Configuration configuration = new Configuration(globalConfiguration);

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

    // Set up notification URL if it's available
    if(request.getNotificationUrl() != null) {
      configuration.set("job.end.notification.url", request.getNotificationUrl());
    }

    // Turn off speculative execution
    //configuration.setBoolean("mapred.map.tasks.speculative.execution", false);
    //configuration.setBoolean("mapred.reduce.tasks.speculative.execution", false);

    // Promote all required jars to the job
    configuration.set("tmpjars", StringUtils.join(request.getJars(), ","));

    try {
      Job job = new Job(configuration);

      // link configs
      MRConfigurationUtils.setConnectorLinkConfig(Direction.FROM, job, request.getConnectorLinkConfig(Direction.FROM));
      MRConfigurationUtils.setConnectorLinkConfig(Direction.TO, job, request.getConnectorLinkConfig(Direction.TO));

      // from and to configs
      MRConfigurationUtils.setConnectorJobConfig(Direction.FROM, job, request.getJobConfig(Direction.FROM));
      MRConfigurationUtils.setConnectorJobConfig(Direction.TO, job, request.getJobConfig(Direction.TO));

      MRConfigurationUtils.setDriverConfig(job, request.getDriverConfig());
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

      // Set number of reducers as number of configured loaders  or suppress
      // reduce phase entirely if loaders are not set at all.
      if (request.getLoaders() != null) {
        job.setNumReduceTasks(request.getLoaders());
      } else {
        job.setNumReduceTasks(0);
      }

      job.setOutputFormatClass(request.getOutputFormatClass());
      job.setOutputKeyClass(request.getOutputKeyClass());
      job.setOutputValueClass(request.getOutputValueClass());


      //JavaPairRDD<SqoopSplit, NullWritable> InitRDD = sc.newAPIHadoopRDD(globalConfiguration,
          //SqoopInputFormatSpark.class, SqoopSplit.class, NullWritable.class);

      //JavaPairRDD<SqoopSplit, SqoopSplit> InitRDD = sc.newAPIHadoopRDD(job.getConfiguration(),
          //SqoopInputFormatSpark.class, SqoopSplit.class, SqoopSplit.class);

      JavaPairRDD<SqoopSplit, NullWritable> InitRDD = sc.newAPIHadoopRDD(job.getConfiguration(),
          SqoopInputFormat.class, SqoopSplit.class, NullWritable.class);

      //scala.Tuple2<SqoopSplit, NullWritable> testFirstTuple = InitRDD.first();
      /*
      InitRDD.map(new Function<Tuple2<SqoopSplit,SqoopSplit>, Object>() {
        @Override
        public Object call(Tuple2<SqoopSplit, SqoopSplit> tuple) throws Exception {
          //Plugin here whatever is done in SqoopMapper's run() (in whatever way possible)
          int i=1;
          i++;
          LOG.info("Inside the Spark map() API");
          LOG.debug("Inside the Spark map() API");
          return null;
        }
      });
      */

      /*
      InitRDD.mapValues(new Function<SqoopSplit, Object>() {
        @Override
        public Object call(SqoopSplit sqoopSplit) throws Exception {
          return null;
        }
      });
      */

      //InitRDD.mapValues(new SqoopMapperSpark());

      InitRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());

      //Trigger the transformation - stub
      //InitRDD.first();

      //Trigger the map() using reduceByKey()
      /*
      InitRDD.reduceByKey(new Function2<SqoopSplit, SqoopSplit, SqoopSplit>() {
        @Override
        public SqoopSplit call(SqoopSplit sqoopSplit, SqoopSplit sqoopSplit2) throws Exception {
          LOG.info("Inside the Spark reduceByKey() API");
          LOG.debug("Inside the Spark reduceByKey() API");
          return null;
        }
      });
      */


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

    //Test stub
    /*
    String logFile = "/Users/banmeet.singh/spark-1.3.1-bin-cdh4/README.md"; // Should be some file on your system
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long numAs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("a"); }
    }).count();

    long numBs = logData.filter(new Function<String, Boolean>() {
      public Boolean call(String s) { return s.contains("b"); }
    }).count();

    LOG.info("Lines with a: " + numAs + ", lines with b: " + numBs);
    */

    sc.stop();

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
