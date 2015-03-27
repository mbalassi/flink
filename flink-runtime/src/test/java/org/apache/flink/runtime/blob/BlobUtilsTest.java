/*
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

package org.apache.flink.runtime.blob;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import java.io.File;

public class BlobUtilsTest {

<<<<<<< HEAD
	private final static String CANNOT_CREATE_THIS = "cannot-create-this";

	private File blobUtilsTestDirectory;

	@Before
	public void before() {
		// Prepare test directory
		blobUtilsTestDirectory = Files.createTempDir();

		assertTrue(blobUtilsTestDirectory.setExecutable(true, false));
		assertTrue(blobUtilsTestDirectory.setReadable(true, false));
		assertTrue(blobUtilsTestDirectory.setWritable(false, false));
	}

	@After
	public void after() {
		// Cleanup test directory
		assertTrue(blobUtilsTestDirectory.delete());
	}

=======
>>>>>>> 3846301d4e945da56acb6e0f5828401c6047c6c2
	@Test(expected = Exception.class)
	public void testExceptionOnCreateStorageDirectoryFailure() {
		// Should throw an Exception
		BlobUtils.initStorageDirectory("/cannot-create-this");
	}

	@Test(expected = Exception.class)
	public void testExceptionOnCreateCacheDirectoryFailure() {
		// Should throw an Exception
		BlobUtils.getStorageLocation(new File("/cannot-create-this"), mock(BlobKey.class));
	}
}
