package org.apache.sqoop.job.mr;

import org.apache.hadoop.io.NullWritable;

import java.io.Serializable;

public class SqoopNullWritable implements Serializable {
  private transient NullWritable nullWritable;
}
