/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.streamvertex;

import org.apache.flink.runtime.event.task.TaskEvent;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;

@SuppressWarnings("rawtypes")
public class RecordOrEvent {

	private TaskEvent event;
	private StreamRecord record;
	private boolean isEvent = false;
	private boolean isRecord = false;

	public RecordOrEvent(TaskEvent event) {
		this.event = event;
		isEvent = true;
	}

	public RecordOrEvent(StreamRecord record) {
		this.record = record;
		isRecord = true;
	}

	public boolean isEvent() {
		return isEvent;
	}

	public boolean isRecord() {
		return isRecord;
	}

	public TaskEvent getEvent() {
		return this.event;
	}

	public StreamRecord getRecord() {
		return this.record;
	}
}
