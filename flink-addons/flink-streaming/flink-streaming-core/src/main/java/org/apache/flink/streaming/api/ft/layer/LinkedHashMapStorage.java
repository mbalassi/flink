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

import java.util.LinkedHashMap;

import org.apache.flink.streaming.api.ft.layer.util.RecordWithHashCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LinkedHashMapStorage implements AbstractPersistenceStorage<Long, RecordWithHashCode> {
	private final static Logger LOG = LoggerFactory.getLogger(LinkedHashMapStorage.class);

	private LinkedHashMap<Long, RecordWithHashCode> backup;

	public LinkedHashMapStorage() {
		this.backup = new LinkedHashMap<Long, RecordWithHashCode>();
	}

	@Override
	public void put(Long id, RecordWithHashCode record) {
		backup.put(id, record);
		if (LOG.isInfoEnabled()) {
			LOG.info("Added to PersistenceLayer: ({},{})", Long.toHexString(id), record);
		}
	}

	@Override
	public RecordWithHashCode get(Long id) {
		return backup.get(id);
	}

	@Override
	public boolean contains(Long id) {
		return backup.containsKey(id);
	}

	@Override
	public void remove(Long id) {
		backup.remove(id);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Removed from PersistenceLayer: {}", Long.toHexString(id));
		}
	}

}
