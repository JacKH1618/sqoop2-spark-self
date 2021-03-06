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
package org.apache.sqoop.handler;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.model.MConnector;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.repository.Repository;
import org.apache.sqoop.repository.RepositoryManager;
import org.apache.sqoop.server.common.ServerError;

public class HandlerUtils {

  public static long getJobIdFromIdentifier(String identifier) {
    // support jobName or jobId for the api
    // NOTE: jobId is a fallback for older sqoop clients if any, since we want
    // to primarily use unique jobNames
    long jobId;
    Repository repository = RepositoryManager.getInstance().getRepository();
    MJob job = repository.findJob(identifier);
    if (job != null) {
      jobId = job.getPersistenceId();
    } else {
      try {
        jobId = Long.valueOf(identifier);
      } catch (NumberFormatException ex) {
        // this means name nor Id existed and we want to throw a user friendly
        // message than a number format exception
        throw new SqoopException(ServerError.SERVER_0005, "Invalid job: " + identifier
            + " requested");
      }
    }
    return jobId;
  }

  public static long getLinkIdFromIdentifier(String identifier) {
    // support linkName or linkId for the api
    // NOTE: linkId is a fallback for older sqoop clients if any, since we want
    // to primarily use unique linkNames
    long linkId;
    Repository repository = RepositoryManager.getInstance().getRepository();
    MLink link = repository.findLink(identifier);
    if (link != null) {
      linkId = link.getPersistenceId();
    } else {
      try {
        linkId = Long.valueOf(identifier);
      } catch (NumberFormatException ex) {
        // this means name nor Id existed and we want to throw a user friendly
        // message than a number format exception
        throw new SqoopException(ServerError.SERVER_0005, "Invalid link: " + identifier
            + " requested");
      }
    }
    return linkId;
  }

  public static long getConnectorIdFromIdentifier(String identifier) {
    long connectorId;
    Repository repository = RepositoryManager.getInstance().getRepository();
    MConnector connector = repository.findConnector(identifier);
    if (connector != null) {
      connectorId = connector.getPersistenceId();
    } else {
      try {
        connectorId = Long.valueOf(identifier);
      } catch (NumberFormatException ex) {
        // this means name nor Id existed and we want to throw a user friendly
        // message than a number format exception
        throw new SqoopException(ServerError.SERVER_0005, "Invalid connector: " + identifier
            + " requested");
      }
    }
    return connectorId;
  }

}
