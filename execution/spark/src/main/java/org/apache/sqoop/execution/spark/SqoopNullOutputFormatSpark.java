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

package org.apache.sqoop.execution.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.log4j.Logger;
import org.apache.sqoop.common.Direction;
import org.apache.sqoop.job.io.SqoopWritable;
import org.apache.sqoop.job.mr.SqoopDestroyerExecutor;
import org.apache.sqoop.job.mr.SqoopOutputFormatLoadExecutor;

import java.io.IOException;

/**
 * An output format for MapReduce job.
 */
public class SqoopNullOutputFormatSpark extends OutputFormat<SqoopWritableListWrapper, NullWritable> {

  public static final Logger LOG = Logger.getLogger(SqoopNullOutputFormatSpark.class);

  @Override
  public void checkOutputSpecs(JobContext context) {
    // do nothing
  }

  @Override
  public RecordWriter<SqoopWritableListWrapper, NullWritable> getRecordWriter(TaskAttemptContext context) {
    SqoopOutputFormatLoadExecutorSpark executor = new SqoopOutputFormatLoadExecutorSpark(context);
    return executor.getRecordWriter();
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) {
    return new SqoopDestroyerOutputCommitter();
  }

  class SqoopDestroyerOutputCommitter extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) {
    }

    @Override
    public void commitJob(JobContext jobContext) throws IOException {
      super.commitJob(jobContext);
      invokeDestroyerExecutor(jobContext, true);
    }

    @Override
    public void abortJob(JobContext jobContext, JobStatus.State state) throws IOException {
      super.abortJob(jobContext, state);
      invokeDestroyerExecutor(jobContext, false);
    }

    private void invokeDestroyerExecutor(JobContext jobContext, boolean success) {
      Configuration config = jobContext.getConfiguration();
      SqoopDestroyerExecutor.executeDestroyer(success, config, Direction.FROM);
      SqoopDestroyerExecutor.executeDestroyer(success, config, Direction.TO);
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) {
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) {
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return false;
    }
  }

}
