/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.ft.layer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.flink.streaming.api.ft.layer.id.RecordId;
import org.apache.flink.streaming.api.ft.layer.util.RecordWithHashCode;
import org.apache.flink.streaming.api.ft.layer.serialization.SemiDeserializedStreamRecord;
import org.apache.flink.streaming.api.ft.layer.util.ExpiredFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTLayer {
	private final static Logger LOG = LoggerFactory.getLogger(FTLayer.class);

	protected AbstractPersistenceLayer<Long, RecordWithHashCode> persistenceLayer;
	protected AckerTable ackerTable;
	protected HashMap<Long, Integer> sourceIdOfRecord;
	protected HashSet<Long> failedSourceRecordIds;
	protected RecordReplayer recordReplayer;

	public FTLayer() {
		// TODO initialize record replayer
		this.sourceIdOfRecord = new HashMap<Long, Integer>();
		this.failedSourceRecordIds = new HashSet<Long>();
		this.persistenceLayer = new TimeoutPersistenceLayer(this, new ExpiredFunction<Long, RecordWithHashCode>() {
			@Override
			public void onExpire(Long sourceRecordId, RecordWithHashCode recordWithHashCode) {
				fail(sourceRecordId, recordWithHashCode);
			}
		}, 5, 100L);
		this.ackerTable = new AckerTable(this);
	}

	public void ack(long sourceRecordId) {
		persistenceLayer.remove(sourceRecordId);
		ackerTable.remove(sourceRecordId);
		if (LOG.isDebugEnabled()) {
			LOG.debug("Source record id {} has been acked", Long.toHexString(sourceRecordId));
		}
	}

	public void explicitFail(long sourceRecordId) {
		fail(sourceRecordId, persistenceLayer.get(sourceRecordId));
	}

	public void fail(long sourceRecordId, RecordWithHashCode recordWithHashCode) {
		if (ackerTable.contains(sourceRecordId)) {
			failedSourceRecordIds.add(sourceRecordId);

			// find sourceRecord
			int sourceId = sourceIdOfRecord.get(sourceRecordId);

			// generate new sourceRecordId
			RecordId newSourceRecordId = RecordId.newSourceRecordId();
			long newId = newSourceRecordId.getSourceRecordId();

			// add new sourceRecord to PersistenceLayer
			sourceIdOfRecord.put(newId, sourceId);
			persistenceLayer.put(newId, recordWithHashCode);

			// add new sourceRecord to AckerTable
			ackerTable.newSourceRecord(newId);

			// delete sourceRecord from PersistenceLayer
			sourceIdOfRecord.remove(sourceRecordId);
			persistenceLayer.remove(sourceRecordId);

			// delete sourceRecordId from AckerTable
			ackerTable.remove(sourceRecordId);

			// create new sourceRecord
			SemiDeserializedStreamRecord newSourceRecord = new SemiDeserializedStreamRecord(
					ByteBuffer.wrap(recordWithHashCode.getSerializedRecord()),
					recordWithHashCode.getHashCode(), newSourceRecordId);

			// send new sourceRecord to sourceSuccessives
			collectToSourceSuccessiveTasks(sourceId, newSourceRecord);
			if (LOG.isDebugEnabled()) {
				LOG.debug("Source record id {} has been failed", Long.toHexString(sourceRecordId));
				LOG.debug("Failed source record id {} is replaced by {}",
						Long.toHexString(sourceRecordId), Long.toHexString(newId));
			}
		}
	}

	public void newSourceRecord(SemiDeserializedStreamRecord sourceRecord, int sourceId) {
		long sourceRecordId = sourceRecord.getId().getSourceRecordId();
		int hashCode = sourceRecord.getHashCode();

		byte[] serializedRecord = sourceRecord.getArray();
		RecordWithHashCode recordWithHashCode = new RecordWithHashCode(serializedRecord, hashCode);

		if (!ackerTable.contains(sourceRecordId)) {
			ackerTable.newSourceRecord(sourceRecordId);
		}
		sourceIdOfRecord.put(sourceRecordId, sourceId);
		persistenceLayer.put(sourceRecordId, recordWithHashCode);
	}

	public void xor(RecordId recordId) {

		long sourceRecordId = recordId.getSourceRecordId();
		if (!failedSourceRecordIds.contains(sourceRecordId)) {
			if (!ackerTable.contains(sourceRecordId)) {
				ackerTable.newSourceRecord(sourceRecordId);
			}
			ackerTable.xor(sourceRecordId, recordId.getCurrentRecordId());
		}
	}

	protected void collectToSourceSuccessiveTasks(int sourceId,
			SemiDeserializedStreamRecord sourceRecord) {

		recordReplayer.replayRecord(sourceId, sourceRecord);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Replaying record to source {} with source record id {}", sourceId,
					Long.toHexString(sourceRecord.getId().getSourceRecordId()));
		}
	}

	protected RecordId generateNewRecordId() {
		return RecordId.newSourceRecordId();
	}

	public boolean isEmpty() {
		return ackerTable.isEmpty();
	}

	public void setRecordReplayer(RecordReplayer recordReplayer) {
		this.recordReplayer = recordReplayer;
	}

	public void open() {
		persistenceLayer.open();
	}

	public void close() {
		persistenceLayer.close();
	}

}