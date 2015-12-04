package org.apache.sqoop.submission.spark;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.connector.matcher.Matcher;
import org.apache.sqoop.connector.matcher.MatcherFactory;
import org.apache.sqoop.error.code.SparkExecutionError;
import org.apache.sqoop.etl.io.DataWriter;
import org.apache.sqoop.execution.spark.SqoopWritableListWrapper;
import org.apache.sqoop.job.etl.Extractor;
import org.apache.sqoop.job.etl.ExtractorContext;
import org.apache.sqoop.mapredsparkcommon.*;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class SparkMapTrigger implements Serializable {

  private JavaPairRDD<SqoopSplit, SqoopSplit> rdd;
  private ConfigurationWrapper wrappedConf;
  private Matcher matcher;
  private IntermediateDataFormat<Object> fromIDF = null;
  private IntermediateDataFormat<Object> toIDF = null;
  //private SqoopWritableListWrapper listWrapper;

  public SparkMapTrigger(JavaPairRDD<SqoopSplit, SqoopSplit> inputRDD, ConfigurationWrapper wrappedConf) {
    this.rdd = inputRDD;
    this.wrappedConf = wrappedConf;
  }

  public JavaPairRDD<IntermediateDataFormat<Object>, Integer> triggerSparkMap() {

    return rdd.mapPartitionsToPair  (new PairFlatMapFunction<Iterator<Tuple2<SqoopSplit, SqoopSplit>>, IntermediateDataFormat<Object>, Integer>() {
      @Override
      public Iterable<Tuple2<IntermediateDataFormat<Object>, Integer>> call(Iterator<Tuple2<SqoopSplit, SqoopSplit>> inputIterator) throws Exception {
        Configuration conf = wrappedConf.getConfiguration();

        String extractorName = conf.get(MRJobConstants.JOB_ETL_EXTRACTOR);
        Extractor extractor = (Extractor) ClassUtils.instantiate(extractorName);

        Schema fromSchema = MRConfigurationUtils.getConnectorSchemaUnsafe(Direction.FROM, conf);
        Schema toSchema = MRConfigurationUtils.getConnectorSchemaUnsafe(Direction.TO, conf);
        matcher = MatcherFactory.getMatcher(fromSchema, toSchema);

        String fromIDFClass = conf.get(MRJobConstants.FROM_INTERMEDIATE_DATA_FORMAT);
        fromIDF = (IntermediateDataFormat<Object>) ClassUtils.instantiate(fromIDFClass);
        fromIDF.setSchema(matcher.getFromSchema());

        // Objects that should be passed to the Executor execution
        PrefixContext subContext = new PrefixContext(conf, MRJobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);
        Object fromConfig = MRConfigurationUtils.getConnectorLinkConfigUnsafe(Direction.FROM, conf);
        Object fromJob = MRConfigurationUtils.getConnectorJobConfigUnsafe(Direction.FROM, conf);

        SqoopMapDataWriterSpark dataWriterSpark = new SqoopMapDataWriterSpark(conf);
        ExtractorContext extractorContext = new ExtractorContext(subContext, dataWriterSpark, fromSchema);

        SqoopSplit split = (inputIterator.next())._1();

        try {
          extractor.extract(extractorContext, fromConfig, fromJob, split.getPartition());
        } catch (Exception e) {
          throw new SqoopException(SparkExecutionError.SPARK_EXEC_0017, e);
        } finally {

        }

        return dataWriterSpark.returnTuplesList;
      }
    }, true);


    /*
    return rdd.flatMapToPair(new PairFlatMapFunction<Tuple2<SqoopSplit, SqoopSplit>, SqoopWritable, Integer>() {
      @Override
      public Iterable<Tuple2<SqoopWritable, Integer>> call(Tuple2<SqoopSplit, SqoopSplit> inputTuple) throws Exception {

        Configuration conf = wrappedConf.getConfiguration();

        String extractorName = conf.get(MRJobConstants.JOB_ETL_EXTRACTOR);
        Extractor extractor = (Extractor) ClassUtils.instantiate(extractorName);

        Schema fromSchema = MRConfigurationUtils.getConnectorSchemaUnsafe(Direction.FROM, conf);
        Schema toSchema = MRConfigurationUtils.getConnectorSchemaUnsafe(Direction.TO, conf);
        matcher = MatcherFactory.getMatcher(fromSchema, toSchema);

        String fromIDFClass = conf.get(MRJobConstants.FROM_INTERMEDIATE_DATA_FORMAT);
        fromIDF = (IntermediateDataFormat<Object>) ClassUtils.instantiate(fromIDFClass);
        fromIDF.setSchema(matcher.getFromSchema());

        // Objects that should be passed to the Executor execution
        PrefixContext subContext = new PrefixContext(conf, MRJobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);
        Object fromConfig = MRConfigurationUtils.getConnectorLinkConfigUnsafe(Direction.FROM, conf);
        Object fromJob = MRConfigurationUtils.getConnectorJobConfigUnsafe(Direction.FROM, conf);

        SqoopMapDataWriterSpark dataWriterSpark = new SqoopMapDataWriterSpark(conf);
        ExtractorContext extractorContext = new ExtractorContext(subContext, dataWriterSpark, fromSchema);

        SqoopSplit split = inputTuple._1();

        try {
          extractor.extract(extractorContext, fromConfig, fromJob, split.getPartition());
        } catch (Exception e) {
          throw new SqoopException(SparkExecutionError.SPARK_EXEC_0017, e);
        } finally {

        }

        return dataWriterSpark.returnTuplesList;
      }
    });
    */

    /*
    return rdd.mapToPair(new PairFunction<Tuple2<SqoopSplit, SqoopSplit>, SqoopWritableListWrapper, Integer>() {
      @Override
      public Tuple2<SqoopWritableListWrapper, Integer> call(Tuple2<SqoopSplit, SqoopSplit> inputTuple) throws Exception {
        Configuration conf = wrappedConf.getConfiguration();

        String extractorName = conf.get(MRJobConstants.JOB_ETL_EXTRACTOR);
        Extractor extractor = (Extractor) ClassUtils.instantiate(extractorName);

        Schema fromSchema = MRConfigurationUtils.getConnectorSchemaUnsafe(Direction.FROM, conf);
        Schema toSchema = MRConfigurationUtils.getConnectorSchemaUnsafe(Direction.TO, conf);
        matcher = MatcherFactory.getMatcher(fromSchema, toSchema);

        String fromIDFClass = conf.get(MRJobConstants.FROM_INTERMEDIATE_DATA_FORMAT);
        fromIDF = (IntermediateDataFormat<Object>) ClassUtils.instantiate(fromIDFClass);
        fromIDF.setSchema(matcher.getFromSchema());

        // Objects that should be passed to the Executor execution
        PrefixContext subContext = new PrefixContext(conf, MRJobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);
        Object fromConfig = MRConfigurationUtils.getConnectorLinkConfigUnsafe(Direction.FROM, conf);
        Object fromJob = MRConfigurationUtils.getConnectorJobConfigUnsafe(Direction.FROM, conf);

        SqoopMapDataWriterSpark dataWriterSpark = new SqoopMapDataWriterSpark(conf);
        ExtractorContext extractorContext = new ExtractorContext(subContext, dataWriterSpark, fromSchema);

        SqoopSplit split = inputTuple._1();

        try {
          extractor.extract(extractorContext, fromConfig, fromJob, split.getPartition());
        } catch (Exception e) {
          throw new SqoopException(SparkExecutionError.SPARK_EXEC_0017, e);
        } finally {

        }

        listWrapper = new SqoopWritableListWrapper(dataWriterSpark.writablesList);
        //NullWritable nullWritable = NullWritable.get();
        Integer dummyValue = new Integer(0xdeadbeef);
        Tuple2<SqoopWritableListWrapper, Integer> returnTuple = new Tuple2<SqoopWritableListWrapper, Integer>(listWrapper, dummyValue);
        return returnTuple;

      }
    });*/

  }


  // There are two IDF objects we carry around in memory during the sqoop job execution.
  // The fromIDF has the fromSchema in it, the toIDF has the toSchema in it.
  // Before we do the writing to the toIDF object we do the matching process to negotiate between
  // the two schemas and their corresponding column types before we write the data to the toIDF object
  private class SqoopMapDataWriterSpark extends DataWriter {
    private Configuration conf;
    //private LinkedList<SqoopWritable> writablesList;
    private List<Tuple2<IntermediateDataFormat<Object>, Integer>> returnTuplesList;

    public SqoopMapDataWriterSpark(Configuration conf) {
      this.conf = conf;
      //this.writablesList = new LinkedList<SqoopWritable>();
      this.returnTuplesList = new LinkedList<>();
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

        String toIDFClass = conf.get(MRJobConstants.TO_INTERMEDIATE_DATA_FORMAT);
        toIDF = (IntermediateDataFormat<Object>) ClassUtils.instantiate(toIDFClass);
        toIDF.setSchema(matcher.getToSchema());
        toIDF.setObjectData(matcher.getMatchingData(fromIDF.getObjectData()));

        //SqoopWritable writable = new SqoopWritable(toIDF);
        Integer dummyValue = new Integer(0xdeadbeef);
        Tuple2<IntermediateDataFormat<Object>, Integer> returnTuple = new Tuple2<>(toIDF, dummyValue);
        returnTuplesList.add(returnTuple);

        // NOTE: We do not use the reducer to do the writing (a.k.a LOAD in ETL). Hence the mapper sets up the writable
        //context.write(writable, NullWritable.get());
      } catch (Exception e) {
        throw new SqoopException(SparkExecutionError.SPARK_EXEC_0013, e);
      }
    }
  }

}
