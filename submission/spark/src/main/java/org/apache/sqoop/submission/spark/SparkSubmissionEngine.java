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
import org.apache.sqoop.job.MRJobConstants;
import org.apache.sqoop.job.PrefixContext;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.job.mr.*;
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
import org.apache.sqoop.utils.ClassUtils;
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

  private /*transient */ IntermediateDataFormat<Object> fromIDF = null;
  private /*transient*/ IntermediateDataFormat<Object> toIDF = null;
  private /*transient*/ Matcher matcher;

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
    SparkConf sparkConf = new SparkConf().setAppName("Sqoop on Spark").setMaster("local")/*.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")/*.set("spark.authenticate", "true"). set("spark.authenticate.secret", "jackh")*/;
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    //sparkConf.set("spark.kryo.classesToRegister", "org.apache.hadoop.conf.Configuration");
    //sparkConf.set("spark.kryo.classesToRegister", "org.apache.sqoop.connector.idf.IntermediateDataFormat");
    //sparkConf.registerKryoClasses(Array(classOf[Configuration]));

    JavaSparkContext sc = new JavaSparkContext(sparkConf);

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

      //Retaining to minimize change to existing functioning code
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


      //JavaPairRDD<SqoopSplit, NullWritable> initRDD = sc.newAPIHadoopRDD(globalConfiguration,
      //SqoopInputFormatSpark.class, SqoopSplit.class, NullWritable.class);

      //JavaPairRDD<SqoopSplit, SqoopSplit> initRDD = sc.newAPIHadoopRDD(job.getConfiguration(),
      //SqoopInputFormatSpark.class, SqoopSplit.class, SqoopSplit.class);

      JavaPairRDD<SqoopSplit, SqoopSplit> initRDD = sc.newAPIHadoopRDD(job.getConfiguration(),
          SqoopInputFormatSpark.class, SqoopSplit.class, SqoopSplit.class);


      /*
      //Moving all the extraction setup code into the driver and passing only the actual extraction
      //to the executor to avoid serialization issues/redundant computation on executors
      String extractorName = job.getConfiguration().get(MRJobConstants.JOB_ETL_EXTRACTOR);
      Extractor extractor = (Extractor) ClassUtils.instantiate(extractorName);

      //Changed to unsafe versions of the getConnectorSchema functions to avoid the (weird) cannot cast
      //Configuration object to JobConf object exception (don't know why this doesn't come elsewhere)
      Schema fromSchema = MRConfigurationUtils.getConnectorSchema(Direction.FROM, job.getConfiguration());
      //Schema fromSchema = request.getJobSubmission().getFromSchema();
      Schema toSchema = MRConfigurationUtils.getConnectorSchema(Direction.TO, job.getConfiguration());
      //Schema toSchema = request.getJobSubmission().getToSchema();

      matcher = MatcherFactory.getMatcher(fromSchema, toSchema);

      String fromIDFClass = job.getConfiguration().get(MRJobConstants.FROM_INTERMEDIATE_DATA_FORMAT);
      fromIDF = (IntermediateDataFormat<Object>) ClassUtils.instantiate(fromIDFClass);
      fromIDF.setSchema(matcher.getFromSchema());
      String toIDFClass = job.getConfiguration().get(MRJobConstants.TO_INTERMEDIATE_DATA_FORMAT);
      toIDF = (IntermediateDataFormat<Object>) ClassUtils.instantiate(toIDFClass);
      toIDF.setSchema(matcher.getToSchema());

      // Objects that should be passed to the Executor execution
      PrefixContext subContext = new PrefixContext(job.getConfiguration(), MRJobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);
      Object fromConfig = MRConfigurationUtils.getConnectorLinkConfigUnsafe(Direction.FROM, job.getConfiguration());
      Object fromJob = MRConfigurationUtils.getConnectorJobConfigUnsafe(Direction.FROM, job.getConfiguration());
      */


      if(false) {
        initRDD.mapValues(new SqoopMapperSpark());
      }

      if (false) {
        //scala.Tuple2<SqoopSplit, NullWritable> testFirstTuple = initRDD.first();
        initRDD.mapValues(new Function<SqoopSplit, Object>() {

          @Override
          public Object call(SqoopSplit split) throws Exception {
            //Plugin here whatever is done in SqoopMapper's run() (in whatever way possible)
            LOG.info("Inside the Spark map() API");
            LOG.debug("Inside the Spark map() API");

            //SqoopSplit split = context.getCurrentKey();
            //ExtractorContext extractorContext = new ExtractorContext(subContext, new SqoopMapDataWriter(context), fromSchema);

            return null;
          }
        });
      }

      if (true) {
        //Create SparkMapTrigger object and use it to trigger mapValues()
        ConfigurationWrapper wrappedConf = new ConfigurationWrapper(job.getConfiguration());
        //String serializedConf = job.getConfiguration().toString();
        //SparkMapTrigger sparkMapTriggerObj = new SparkMapTrigger(initRDD, /*new Integer(404)*/ /*fromIDF*/ /*serializedConf*/ wrappedConf job.getConfiguration()*//*, fromIDF, toIDF*/);
        SparkMapTrigger sparkMapTriggerObj = new SparkMapTrigger(initRDD, wrappedConf);
        sparkMapTriggerObj.triggerSparkMapValues();
      }

      /*
      initRDD.mapValues(new Function<SqoopSplit, SqoopSplit> () {

        private IntermediateDataFormat<Object> fromIDF = null;
        private IntermediateDataFormat<Object> toIDF = null;
        private Matcher matcher;

        @Override
        public Object call(Tuple2<SqoopSplit, NullWritable> tuple) throws Exception {
          //Plugin here whatever is done in SqoopMapper's run() (in whatever way possible)
          LOG.info("Inside the Spark map() API");
          LOG.debug("Inside the Spark map() API");

          String extractorName = configuration.get(MRJobConstants.JOB_ETL_EXTRACTOR);
          Extractor extractor = (Extractor) ClassUtils.instantiate(extractorName);

          Schema fromSchema = MRConfigurationUtils.getConnectorSchema(Direction.FROM, configuration);
          Schema toSchema = MRConfigurationUtils.getConnectorSchema(Direction.TO, configuration);
          matcher = MatcherFactory.getMatcher(fromSchema, toSchema);

          String fromIDFClass = configuration.get(MRJobConstants.FROM_INTERMEDIATE_DATA_FORMAT);
          fromIDF = (IntermediateDataFormat<Object>) ClassUtils.instantiate(fromIDFClass);
          fromIDF.setSchema(matcher.getFromSchema());
          String toIDFClass = configuration.get(MRJobConstants.TO_INTERMEDIATE_DATA_FORMAT);
          toIDF = (IntermediateDataFormat<Object>) ClassUtils.instantiate(toIDFClass);
          toIDF.setSchema(matcher.getToSchema());

          return null;
        }
      });
      */

      /*
      initRDD.mapValues(new Function<SqoopSplit, Object>() {
        @Override
        public Object call(SqoopSplit sqoopSplit) throws Exception {
          return null;
        }
      });
      */

      //initRDD.mapValues(new SqoopMapperSpark());

      initRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());

      //Trigger the transformation - stub
      //initRDD.first();

      //Trigger the map() using reduceByKey()
      /*
      initRDD.reduceByKey(new Function2<SqoopSplit, SqoopSplit, SqoopSplit>() {
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
