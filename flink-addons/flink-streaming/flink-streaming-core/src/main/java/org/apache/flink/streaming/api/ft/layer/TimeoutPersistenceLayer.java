/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.ft.layer;

import org.apache.flink.streaming.api.ft.layer.util.RecordWithHashCode;
import org.apache.flink.streaming.api.ft.layer.util.ExpiredFunction;

public class TimeoutPersistenceLayer extends AbstractPersistenceLayer<Long, RecordWithHashCode> {
	private static final long serialVersionUID = 1L;

	private RotatingHashMap<Long, RecordWithHashCode> rotatingHashMap;
	private long recordTimeout;

	private TimeoutHandler timeoutHandler;

	public TimeoutPersistenceLayer(FTLayer ftLayer, ExpiredFunction<Long, RecordWithHashCode> expiredFunction, int numberOfBuckets, long recordTimeout) {
		super(ftLayer);
		this.rotatingHashMap = new RotatingHashMap<Long, RecordWithHashCode>(expiredFunction, numberOfBuckets);

		this.storage = rotatingHashMap;
		this.recordTimeout = recordTimeout;
	}

	public void timeout() {
		rotatingHashMap.rotate();
	}

	@Override
	public void open() {
		startTimeoutHandling();
	}

	@Override
	public void close() {
		stopTimeoutHandling();
	}

	private class TimeoutHandler extends Thread {
		private boolean running = true;

		public void terminate() {
			running = false;
		}

		@Override
		public void run() {
			while (running) {
				try {
					Thread.sleep(recordTimeout);
					timeout();
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}

	private void startTimeoutHandling() {
		timeoutHandler = new TimeoutHandler();
		timeoutHandler.start();
	}

	private void stopTimeoutHandling() {
		try {
			if (timeoutHandler != null) {
				timeoutHandler.terminate();
				timeoutHandler.join();
			}
		} catch (Exception e) {
			throw new RuntimeException("Cannot terminate timeout handler", e);
		}
	}

}
