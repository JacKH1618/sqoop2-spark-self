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

/**
 * Configuration constants for Spark submission engine
 */
public class Constants {

  public static final String PREFIX_SPARK = "spark.";

  public static final String CONF_CONFIG_DIR =
    PREFIX_SPARK + "configuration.directory";

  public static final String SQOOP_JOB = "sqoop.job";

  private Constants() {
    // Instantiation is prohibited
  }
}
