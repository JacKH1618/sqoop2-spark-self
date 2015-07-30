package org.apache.sqoop.submission.spark;

import org.apache.log4j.Logger;
import org.apache.sqoop.connector.idf.IntermediateDataFormat;
import org.apache.sqoop.job.mr.SqoopSplit;
import org.apache.sqoop.connector.matcher.Matcher;
import java.io.Serializable;
import org.apache.spark.api.java.function.Function;

public class SqoopMapperSpark implements Function<SqoopSplit,Object>, Serializable {

  private static Logger LOG = Logger.getLogger(SqoopMapperSpark.class);
  private /*transient */ IntermediateDataFormat<Object> fromIDF = null;
  private /*transient*/ IntermediateDataFormat<Object> toIDF = null;
  private /*transient*/ Matcher matcher;

  @Override
  public Object call(SqoopSplit split) throws Exception {

    //Plugin here whatever is done in SqoopMapper's run() (in whatever way possible)
    LOG.info("Inside the Spark map() API");
    LOG.debug("Inside the Spark map() API");
    /*

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
    */

    return null;
  }
}
