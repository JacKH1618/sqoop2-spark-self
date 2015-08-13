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

// jackh: Might need to change these for Spark instead of MR

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.sqoop.driver.JobRequest;
import org.apache.sqoop.mapredsparkcommon.SqoopWritable;

import java.util.LinkedList;

/**
 * Spark specific submission request containing all extra information
 * needed for bootstrapping Spark job.
 */
public class SparkJobRequest extends JobRequest {

  /**
   * Options relevant to Spark
   */
  Class<? extends InputFormat> inputFormatClass;
  Class<? extends OutputFormat> outputFormatClass;

  // Building a List wrapper around the outputKeyClass
  // Later improvement: The Spark engine, the way it is currently written, needs a list of SqoopWritable
  // like stuff as the key class. This is because the mapPair() API will return a tuple per partition
  // due to which the key of this tuple cannot be just one row, but must be a collection of rows.
  // Can probably make an interface to ensure this.

  Class outputKeyClass;

  Class<? extends Writable> outputValueClass;

  public SparkJobRequest() {
    super();
  }

  public Class<? extends InputFormat> getInputFormatClass() {
    return inputFormatClass;
  }

  public void setInputFormatClass(Class<? extends InputFormat> inputFormatClass) {
    this.inputFormatClass = inputFormatClass;
  }

  public Class<? extends OutputFormat> getOutputFormatClass() {
    return outputFormatClass;
  }

  public void setOutputFormatClass(Class<? extends OutputFormat> outputFormatClass) {
    this.outputFormatClass = outputFormatClass;
  }

  //public Class<? extends Writable> getOutputKeyClass() {
  public Class<LinkedList<SqoopWritable>> getOutputKeyClass() {
    return outputKeyClass;
  }

  //public void setOutputKeyClass(Class<? extends Writable> outputKeyClass) {
  public void setOutputKeyClass(Class outputKeyClass) {
    this.outputKeyClass = outputKeyClass;
  }

  public Class<? extends Writable> getOutputValueClass() {
    return outputValueClass;
  }

  public void setOutputValueClass(Class<? extends Writable> outputValueClass) {
    this.outputValueClass = outputValueClass;
  }
}