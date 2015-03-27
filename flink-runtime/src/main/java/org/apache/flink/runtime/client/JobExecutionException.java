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


package org.apache.flink.runtime.client;

import org.apache.flink.runtime.jobgraph.JobID;

/**
<<<<<<< HEAD
 * This exception is the base exception for all exceptions that denote any failure during
 * teh execution of a job. The JobExecutionException and its subclasses are thrown by
 * the {@link JobClient}.
=======
 * This exception is thrown by the {@link JobClient} if a Nephele job has been aborted either as a result of a user
 * request or an error which occurred during the execution.
 * 
>>>>>>> 3846301d4e945da56acb6e0f5828401c6047c6c2
 */
public class JobExecutionException extends Exception {

	/**
	 * The generated serial UID.
	 */
	private static final long serialVersionUID = 2818087325120827525L;

	private JobID jobID;

	/**
	 * Indicates whether the job has been aborted as a result of user request.
	 */
	private final boolean canceledByUser;

	/**
	 * Constructs a new job execution exception.
	 * 
	 * @param msg
	 *        the message that shall be encapsulated by this exception
	 * @param canceledByUser
	 *        <code>true</code> if the job has been aborted as a result of a user request, <code>false</code> otherwise
	 */
<<<<<<< HEAD
	public JobExecutionException(final JobID jobID, final String msg, final Throwable cause) {
		super(msg, cause);

		this.jobID = jobID;
	}

	public JobExecutionException(final JobID jobID, final String msg) {
		super(msg);

		this.jobID = jobID;
	}

	public JobExecutionException(final JobID jobID, final Throwable cause) {
		super(cause);

		this.jobID = jobID;
	}

	public JobID getJobID() {
		return jobID;
=======
	public JobExecutionException(final String msg, final boolean canceledByUser) {
		super(msg);

		this.canceledByUser = canceledByUser;
	}

	/**
	 * Returns <code>true</code> if the job has been aborted as a result of a user request, <code>false</code>
	 * otherwise.
	 * 
	 * @return <code>true</code> if the job has been aborted as a result of a user request, <code>false</code> otherwise
	 */
	public boolean isJobCanceledByUser() {

		return this.canceledByUser;
>>>>>>> 3846301d4e945da56acb6e0f5828401c6047c6c2
	}
}
