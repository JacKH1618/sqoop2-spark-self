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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.MapContext;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.driver.SubmissionEngine;
import org.apache.sqoop.error.code.SparkSubmissionError;
import org.apache.sqoop.execution.spark.SparkExecutionEngine;
import org.apache.sqoop.execution.spark.SparkJobRequest;
import org.apache.sqoop.execution.spark.SqoopInputFormatSpark;
import org.apache.sqoop.execution.spark.SqoopWritableListWrapper;
import org.apache.sqoop.mapredsparkcommon.MRConfigurationUtils;
import org.apache.sqoop.mapredsparkcommon.MRJobConstants;
import org.apache.sqoop.mapredsparkcommon.SqoopSplit;
import org.apache.sqoop.mapredsparkcommon.SqoopWritable;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.SubmissionError;
import org.apache.sqoop.submission.SubmissionStatus;

import java.io.File;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.util.Map;


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

      job.setOutputFormatClass(request.getOutputFormatClass());
      job.setOutputKeyClass(request.getOutputKeyClass());
      job.setOutputValueClass(request.getOutputValueClass());

      // Form the initial RDD from the Hadoop configuration object set up above
      JavaPairRDD<SqoopSplit, SqoopSplit> initRDD = sc.newAPIHadoopRDD(job.getConfiguration(),
          SqoopInputFormatSpark.class, SqoopSplit.class, SqoopSplit.class);

      // For debugging - check size of initial RDD; remove in production
      int numPartitions = initRDD.partitions().size();

      // Create SparkMapTrigger object and use it to trigger mapToPair()
      ConfigurationWrapper wrappedConf = new ConfigurationWrapper(job.getConfiguration());
      SparkMapTrigger sparkMapTriggerObj = new SparkMapTrigger(initRDD, wrappedConf);
      JavaPairRDD<IntermediateDataFormat<Object>, Integer> mappedRDD = sparkMapTriggerObj.triggerSparkMap();

      // Add reduce phase/any transformation code here
      // For debugging - check size of RDD before partitioning; remove in production
      numPartitions = mappedRDD.partitions().size();

      JavaPairRDD<IntermediateDataFormat<Object>, Integer> repartitionedRDD = null;

      // Get number of loaders, if specified
      if(request.getLoaders() != null) {
        long numLoaders = request.getLoaders();
        long numExtractors = (request.getExtractors() != null) ?
          (request.getExtractors()) : (job.getConfiguration().getLong(MRJobConstants.JOB_ETL_EXTRACTOR_NUM, 10));

        if(numLoaders > numExtractors) {
          // Repartition the RDD: yields evenly balanced partitions but has a shuffle cost
          repartitionedRDD = mappedRDD.repartition(request.getLoaders());
        }
        else if(numLoaders < numExtractors) {
          // Use coalesce() in this case. Shuffle tradeoff: turning shuffle on will give us evenly balanced partitions
          // leading to an optimum write time but will incur network costs; shuffle off rids us of the network cost
          // but might lead to sub-optimal write performance if the partitioning by the InputFormar was skewed in the
          // first place
          repartitionedRDD = mappedRDD.coalesce(request.getLoaders(), false);
        }
        else {
          // Do not do any repartitioning/coalescing if loaders were specified but were equal to extractors
          // Check if this statement incurs any cost
          repartitionedRDD = mappedRDD;
        }
      }
      else {
        // Do not do any repartitioning/coalescing no loaders are specified
        repartitionedRDD = mappedRDD;
      }

      // For debugging - check size of RDD after partitioning; remove in production
      numPartitions = repartitionedRDD.partitions().size();

      // Calls the OutputFormat for writing
      //mappedRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());
      repartitionedRDD.saveAsNewAPIHadoopDataset(job.getConfiguration());

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
