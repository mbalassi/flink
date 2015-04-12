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

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.runtime.state.OperatorState;
import org.apache.flink.runtime.state.StateHandle;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OperatorStateTest2 {

	private Map<String,OperatorState<?>> states = new HashMap<String,OperatorState<?>>();
	private final String foo = "foo";
	private final OperatorState<String> bar = new OperatorState<String>("bar");

	@Test
	public void testOperatorState() throws IOException {

		states.put(foo, bar);
		StateHandle handle = new LocalStateHandle(states);
		byte[] rawHandle = SerializationUtils.serialize(handle);
		StateHandle restoredHandle = (StateHandle) SerializationUtils.deserialize(rawHandle);
		states.get(foo).update("qux");
		assertTrue(bar.stateEquals((OperatorState<String>) 
				restoredHandle.getState(ClassLoader.getSystemClassLoader()).get(foo)));

	}

}
