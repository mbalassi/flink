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
package org.apache.flink.api.expressions

import org.apache.flink.runtime.jobgraph.JobID;

/**
 * The operations in this package are created by calling methods on [[ExpressionOperation]] they
 * should not be manually created by users of the API.
 */
<<<<<<< HEAD:flink-runtime/src/main/java/org/apache/flink/runtime/client/JobTimeoutException.java
public class JobTimeoutException extends JobExecutionException {

	private static final long serialVersionUID = 2818087325120827529L;

	public JobTimeoutException(final JobID jobID, final String msg, final Throwable cause) {
		super(jobID, msg, cause);
	}
}
=======
package object operations
>>>>>>> 3846301d4e945da56acb6e0f5828401c6047c6c2:flink-staging/flink-linq/src/main/scala/org/apache/flink/api/expressions/operations/package.scala
