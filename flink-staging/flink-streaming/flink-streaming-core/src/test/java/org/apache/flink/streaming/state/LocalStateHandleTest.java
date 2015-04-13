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

package org.apache.flink.streaming.state;

import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.runtime.state.StateCheckpoint;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LocalStateHandleTest {

	private static final Logger LOG = LoggerFactory.getLogger(LocalStateHandle.class);

	@Test
	public void testStateHandle() {
		Map<String, OperatorState<?>> mapState = new HashMap<String, OperatorState<?>>();

		mapState.put("test", new OperatorState<Object>(new Long(0)));
		mapState.put("flink", new OperatorState<Object>(new Long(1)));
		mapState.put("streaming", new OperatorState<Object>(new Long(2)));

		LocalStateHandle handle = null;

		try {
			 handle = new LocalStateHandle(mapState);
		} catch (IOException e) {
			LOG.error("State handle was not crated correctly", mapState);
			fail();
		}

		// This should have no effect on the state handle
		mapState.put("no effect", new OperatorState<Object>(new Long(3)));

		Map<String, OperatorState<?>> handleState = handle.getState(Thread.currentThread().getContextClassLoader());

		assertEquals(3, handleState.size());
		assertEquals(new HashSet(Arrays.asList("test", "flink", "streaming")), handleState.keySet());
	}

}
