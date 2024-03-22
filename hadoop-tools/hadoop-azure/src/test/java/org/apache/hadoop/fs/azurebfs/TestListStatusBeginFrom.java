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

package org.apache.hadoop.fs.azurebfs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.Assume;
import org.junit.Test;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.azurebfs.constants.TestConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.services.AuthType;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

public class TestListStatusBeginFrom extends AbstractAbfsIntegrationTest {

  public TestListStatusBeginFrom() throws Exception {
    // The mock SAS token provider relies on the account key to generate SAS.
    Assume.assumeTrue(this.getAuthType() == AuthType.SharedKey);
  }

  @Override
  public void setup() throws Exception {
    boolean isHNSEnabled = this.getConfiguration().getBoolean(
        TestConfigurationKeys.FS_AZURE_TEST_NAMESPACE_ENABLED_ACCOUNT, false);
    Assume.assumeTrue(isHNSEnabled);
    loadConfiguredFileSystem();
    super.setup();
  }

  @Test
  public void testFileStatusOnRoot() throws Exception {
    AzureBlobFileSystem fs = getFileSystem();

    Path directory = fs.makeQualified(new Path("/mount/newcontainer/testcontainer"));
    Path startFrom = fs.makeQualified(new Path("testcontainer"));
    boolean useBeginFrom = true;
    boolean recursive = false;


//    List<FileStatus> fileStatuses = new ArrayList<>();
//    String continuation = null;
//    boolean fetchAll = true;
    TracingContext tc = getTestTracingContext(fs, true);
    Iterator<FileStatus> itr = fs.getAbfsStore().listStatusAsIterator(directory, startFrom, tc);
    itr.next();
    itr.next();
    itr.next();
    itr.next();
    itr.next();
//    fs.getAbfsStore().listStatus(directory, startFrom, fileStatuses, fetchAll, continuation, tc, useBeginFrom, recursive);

  }

}
