package org.apache.sqoop.submission.spark;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.error.code.MRExecutionError;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.job.mr.SqoopSplit;

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

  public Object triggerSparkMapValues() {
    rdd.mapValues(new Function<SqoopSplit, SqoopSplit>() {

      @Override
      public SqoopSplit call(SqoopSplit split) throws Exception {
        //Plugin here whatever is done in SqoopMapper's run() (in whatever way possible)
        //LOG.info("Inside the Spark map() API");
        //LOG.debug("Inside the Spark map() API");

        //SqoopSplit split = context.getCurrentKey();
        //ExtractorContext extractorContext = new ExtractorContext(subContext, new SqoopMapDataWriter(context), fromSchema);

        //String extractorName = wrappedConf.get(MRJobConstants.JOB_ETL_EXTRACTOR);

        return null;
      }
    });
    return null;
  }

  /*
  // There are two IDF objects we carry around in memory during the sqoop job execution.
  // The fromIDF has the fromSchema in it, the toIDF has the toSchema in it.
  // Before we do the writing to the toIDF object we do the matching process to negotiate between
  // the two schemas and their corresponding column types before we write the data to the toIDF object
  private class SqoopMapDataWriter extends DataWriter {
    private Mapper.Context context;
    private SqoopWritable writable;

    public SqoopMapDataWriter(Mapper.Context context) {
      this.context = context;
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
      try {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Extracted data: " + fromIDF.getCSVTextData());
        }
        // NOTE: The fromIDF and the corresponding fromSchema is used only for the matching process
        // The output of the mappers is finally written to the toIDF object after the matching process
        // since the writable encapsulates the toIDF ==> new SqoopWritable(toIDF)
        toIDF.setObjectData(matcher.getMatchingData(fromIDF.getObjectData()));
        // NOTE: We do not use the reducer to do the writing (a.k.a LOAD in ETL). Hence the mapper sets up the writable
        context.write(writable, NullWritable.get());
      } catch (Exception e) {
        throw new SqoopException(MRExecutionError.MAPRED_EXEC_0013, e);
      }
    }
  }
  */

}
