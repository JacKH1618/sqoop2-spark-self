package org.apache.sqoop.execution.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.SparkExecutionError;
import org.apache.sqoop.mapredsparkcommon.MRJobConstants;
import org.apache.sqoop.job.PrefixContext;
import org.apache.sqoop.job.etl.Partition;
import org.apache.sqoop.job.etl.PartitionerContext;
import org.apache.sqoop.mapredsparkcommon.MRConfigurationUtils;
import org.apache.sqoop.job.mr.SqoopSplit;
import org.apache.sqoop.schema.Schema;
import org.apache.sqoop.utils.ClassUtils;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * An InputFormat for MapReduce job.
 */
public class SqoopInputFormatSpark extends InputFormat<SqoopSplit, SqoopSplit> {

  public static final Logger LOG =
      Logger.getLogger(SqoopInputFormatSpark.class);

  @Override
  public RecordReader<SqoopSplit, SqoopSplit> createRecordReader(
      InputSplit split, TaskAttemptContext context) {
    return new SqoopRecordReader();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public List<InputSplit> getSplits(JobContext context)
      throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    String partitionerName = conf.get(MRJobConstants.JOB_ETL_PARTITIONER);
    org.apache.sqoop.job.etl.Partitioner partitioner = (org.apache.sqoop.job.etl.Partitioner) ClassUtils.instantiate(partitionerName);

    PrefixContext connectorContext = new PrefixContext(conf, MRJobConstants.PREFIX_CONNECTOR_FROM_CONTEXT);
    Object connectorLinkConfig = MRConfigurationUtils.getConnectorLinkConfig(Direction.FROM, conf);
    Object connectorFromJobConfig = MRConfigurationUtils.getConnectorJobConfig(Direction.FROM, conf);
    Schema fromSchema = MRConfigurationUtils.getConnectorSchema(Direction.FROM, conf);

    long maxPartitions = conf.getLong(MRJobConstants.JOB_ETL_EXTRACTOR_NUM, 10);
    PartitionerContext partitionerContext = new PartitionerContext(connectorContext, maxPartitions, fromSchema);

    List<Partition> partitions = partitioner.getPartitions(partitionerContext, connectorLinkConfig, connectorFromJobConfig);
    List<InputSplit> splits = new LinkedList<InputSplit>();
    for (Partition partition : partitions) {
      LOG.debug("Partition: " + partition);
      SqoopSplit split = new SqoopSplit();
      split.setPartition(partition);
      splits.add(split);
    }

    // SQOOP-2382: Need to skip this check in case extractors is set to 1
    // and null values are allowed in partitioning column
    if(splits.size() > maxPartitions && (false == partitionerContext.getSkipMaxPartitionCheck())) {
      throw new SqoopException(SparkExecutionError.SPARK_EXEC_0025,
          String.format("Got %d, max was %d", splits.size(), maxPartitions));
    }

    return splits;
  }

  public static class SqoopRecordReader
      extends RecordReader<SqoopSplit, SqoopSplit> {

    private boolean delivered = false;
    private SqoopSplit split = null;

    @Override
    public boolean nextKeyValue() {
      if (delivered) {
        return false;
      } else {
        delivered = true;
        return true;
      }
    }

    @Override
    public SqoopSplit getCurrentKey() {
      return split;
    }

    @Override
    public SqoopSplit getCurrentValue() {
      return split;
    }

    @Override
    public void close() {
    }

    @Override
    public float getProgress() {
      return delivered ? 1.0f : 0.0f;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) {
      this.split = (SqoopSplit)split;
    }
  }

}
