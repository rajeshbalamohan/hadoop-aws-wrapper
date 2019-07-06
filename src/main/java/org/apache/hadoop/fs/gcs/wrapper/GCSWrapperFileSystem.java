/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License.  You may obtain
 * a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.fs.gcs.wrapper;

import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import org.apache.hadoop.fs.generic.wrapper.GenericWrappedFileSystem;

/**
 * GCS Wrapper which logs all FS calls for future reference.
 * <pre>
 *   e.g
 *   ./dist/hive/bin/hive --database rajesh --hiveconf fs.gs.impl=org.apache.hadoop.fs.gcs.wrapper.GCSWrapperFileSystem
 *   --hiveconf fs.gs.project.id=test-144806 --hiveconf fs.gs.working.dir=/
 *   --hiveconf fs.gs.auth.service.account.email=test@test-144806.iam.gserviceaccount.com
 *   --hiveconf fs.gs.auth.service.account.enable=true
 *   --hiveconf fs.gs.auth.service.account.keyfile=/tmp/test.p12
 *   --hiveconf hive.metastore.uris=""
 *   --hiveconf hive.tez.log.level=INFO
 *   --hiveconf tez.task.log.level=INFO
 *
 * hive> set hive.execution.mode=container;
 *
 * hive> add jar file:////home/rbalamohan/bigdata-interop/gcs/target/gcs-connector-1.5.6-hadoop2-SNAPSHOT-shaded.jar;
 * Added [file:////home/rbalamohan/bigdata-interop/gcs/target/gcs-connector-1.5.6-hadoop2-SNAPSHOT-shaded.jar] to class path
 * Added resources: [file:////home/rbalamohan/bigdata-interop/gcs/target/gcs-connector-1.5.6-hadoop2-SNAPSHOT-shaded.jar]
 *
 * hive> add jar file:///tmp/hadoop-aws-wrapper-2.7.0.jar;
 * Added [file:///tmp/hadoop-aws-wrapper-2.7.0.jar] to class path
 * Added resources: [file:///tmp/hadoop-aws-wrapper-2.7.0.jar]
 *
 * hive> select count(*) c from inventory group by inv_item_sk order by c limit 10;
 *
 * </pre>
 */
public class GCSWrapperFileSystem extends GenericWrappedFileSystem {

  public GCSWrapperFileSystem() {
    super(new GoogleHadoopFileSystem());
  }
}

