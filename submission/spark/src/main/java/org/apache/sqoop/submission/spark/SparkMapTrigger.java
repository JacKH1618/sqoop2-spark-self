package org.apache.sqoop.submission.spark;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.matcher.Matcher;
import org.apache.sqoop.connector.matcher.MatcherFactory;
import org.apache.sqoop.error.code.MRExecutionError;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.job.MRJobConstants;
import org.apache.sqoop.job.PrefixContext;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.job.mr.MRConfigurationUtils;
import org.apache.sqoop.job.mr.SqoopSplit;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;
import scala.Tuple2;

import java.io.DataOutputStream;
import java.io.Serializable;

public class SparkMapTrigger implements Serializable {

  private JavaPairRDD<SqoopSplit,SqoopSplit> rdd;
  //private IntermediateDataFormat<Object> fromIDF = null;
  //private Integer i;
  //private IntermediateDataFormat<Object> toIDF = null;
  private ConfigurationWrapper wrappedConf;
  //private Configuration conf;
  //private Job job;
  //private String serializedConf;
  private Matcher matcher;
  private IntermediateDataFormat<Object> fromIDF = null;
  private IntermediateDataFormat<Object> toIDF = null;

  //public SparkMapTrigger(JavaPairRDD<SqoopSplit,SqoopSplit> inputRDD, Integer i /*String serializedConf /*ConfigurationWrapper wrappedConf Configuration conf,*/ /*IntermediateDataFormat<Object> fromIDF*//*, IntermediateDataFormat<Object> toIDF*/) {
  public SparkMapTrigger(JavaPairRDD<SqoopSplit, SqoopSplit> inputRDD, ConfigurationWrapper wrappedConf) {
    this.rdd = inputRDD;
    //this.i = i;
    //this.serializedConf = serializedConf;
    //this.job = job;
    this.wrappedConf = wrappedConf;
    //this.conf = conf;
    //this.fromIDF = fromIDF;
    //this.toIDF = toIDF;
  }

  public JavaPairRDD<SqoopWritable,NullWritable> triggerSparkMapValues() {
    //return rdd.mapValues(new Function<SqoopSplit, Integer>() {
    return rdd.mapToPair(new PairFunction<Tuple2<SqoopSplit, SqoopSplit>, SqoopWritable, NullWritable>() {
      @Override
      public Tuple2<SqoopWritable, NullWritable> call(Tuple2<SqoopSplit, SqoopSplit> inputTuple) throws Exception {
        Configuration conf = wrappedConf.getConfiguration();

        String extractorName = conf.get(MRJobConstants.JOB_ETL_EXTRACTOR);
        Extractor extractor = (Extractor) ClassUtils.instantiate(extractorName);

        Schema fromSchema = MRConfigurationUtils.getConnectorSchemaUnsafe(Direction.FROM, conf);
        Schema toSchema = MRConfigurationUtils.getConnectorSchemaUnsafe(Direction.TO, conf);
        matcher = MatcherFactory.getMatcher(fromSchema, toSchema);

        String fromIDFClass = conf.get(MRJobConstants.FROM_INTERMEDIATE_DATA_FORMAT);
        fromIDF = (IntermediateDataFormat<Object>) ClassUtils.instantiate(fromIDFClass);
        fromIDF.setSchema(matcher.getFromSchema());
        String toIDFClass = conf.get(MRJobConstants.TO_INTERMEDIATE_DATA_FORMAT);
        toIDF = (IntermediateDataFormat<Object>) ClassUtils.instantiate(toIDFClass);
        toIDF.setSchema(matcher.getToSchema());

        // Objects that should be passed to the Executor execution
        PrefixContext subContext = new PrefixContext(conf, MRJobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);
        Object fromConfig = MRConfigurationUtils.getConnectorLinkConfigUnsafe(Direction.FROM, conf);
        Object fromJob = MRConfigurationUtils.getConnectorJobConfigUnsafe(Direction.FROM, conf);

        SqoopMapDataWriterSpark dataWriterSpark = new SqoopMapDataWriterSpark();
        ExtractorContext extractorContext = new ExtractorContext(subContext, dataWriterSpark /*new SqoopMapDataWriterSpark(context)*/, fromSchema);

        SqoopSplit split = inputTuple._1();

        try {
          extractor.extract(extractorContext, fromConfig, fromJob, split.getPartition());
        } catch (Exception e) {
          throw new SqoopException(MRExecutionError.MAPRED_EXEC_0017, e);
        } finally {

        }

        NullWritable nullWritable = NullWritable.get();
        Tuple2<SqoopWritable, NullWritable> returnTuple = new Tuple2<SqoopWritable, NullWritable>(dataWriterSpark.writable, nullWritable);
        return returnTuple;

        //jackh: test stub
        /*
        SqoopWritable writable = new SqoopWritable();
        NullWritable nullWritable = NullWritable.get();
        Tuple2<SqoopWritable, NullWritable> returnTuple = new Tuple2<SqoopWritable, NullWritable>(writable, nullWritable);
        return returnTuple;
        */
      }
    });

      /*
      @Override
      public Integer call(SqoopSplit split) throws Exception {
        //Plugin here whatever is done in SqoopMapper's run() (in whatever way possible)
        //LOG.info("Inside the Spark map() API");
        //LOG.debug("Inside the Spark map() API");

        //SqoopSplit split = context.getCurrentKey();
        //ExtractorContext extractorContext = new ExtractorContext(subContext, new SqoopMapDataWriter(context), fromSchema);

        //String extractorName = wrappedConf.get(MRJobConstants.JOB_ETL_EXTRACTOR);
        Configuration conf = wrappedConf.getConfiguration();

        return new Integer(404);
      }
    });
    */
    //return null;
  }


  // There are two IDF objects we carry around in memory during the sqoop job execution.
  // The fromIDF has the fromSchema in it, the toIDF has the toSchema in it.
  // Before we do the writing to the toIDF object we do the matching process to negotiate between
  // the two schemas and their corresponding column types before we write the data to the toIDF object
  private class SqoopMapDataWriterSpark extends DataWriter {
    //private Mapper.Context context;
    private SqoopWritable writable;

    public SqoopMapDataWriterSpark(/*Mapper.Context context*/) {
      //this.context = context;
      this.writable = new SqoopWritable(toIDF);
    }

    @Override
    public void writeArrayRecord(Object[] array) {
      fromIDF.setObjectData(array);
      writeContent();
    }

    @Override
    public void writeStringRecord(String text) {
      fromIDF.setCSVTextData(text);
      writeContent();
    }

    @Override
    public void writeRecord(Object obj) {
      fromIDF.setData(obj);
      writeContent();
    }

    private void writeContent() {
      //jackh: check logging later

      try {
        /*
        if (LOG.isDebugEnabled()) {
          LOG.debug("Extracted data: " + fromIDF.getCSVTextData());
        }
        */
      // NOTE: The fromIDF and the corresponding fromSchema is used only for the matching process
      // The output of the mappers is finally written to the toIDF object after the matching process
      // since the writable encapsulates the toIDF ==> new SqoopWritable(toIDF)
      toIDF.setObjectData(matcher.getMatchingData(fromIDF.getObjectData()));
      // NOTE: We do not use the reducer to do the writing (a.k.a LOAD in ETL). Hence the mapper sets up the writable
      //context.write(writable, NullWritable.get());
    } catch (Exception e) {
      throw new SqoopException(MRExecutionError.MAPRED_EXEC_0013, e);
    }
  }
}

}
