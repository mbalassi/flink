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

package org.apache.flink.streaming.api.ft;

import org.apache.flink.streaming.api.ft.layer.AbstractPersistenceLayer;
import org.apache.flink.streaming.api.ft.layer.AckerTable;
import org.apache.flink.streaming.api.ft.layer.FTLayer;
import org.apache.flink.streaming.api.ft.layer.id.RecordId;
import org.apache.flink.streaming.api.ft.layer.util.RecordWithHashCode;
import org.apache.flink.streaming.api.ft.layer.serialization.SemiDeserializedStreamRecord;
import org.junit.Test;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

// Testing the functions of the FTLayer to show that the backing AckerTable and PersistenceLayer functions correctly
public class FTLayerTest {

	private static ArrayDeque<Long> ackedRecordIds;
	private static ArrayDeque<Long> failedRecordIds;
	// for every source, stores a list of semi deserialized stream records that would be replayed from that source
	private static HashMap<Integer, ArrayDeque<SemiDeserializedStreamRecord>> replayedRecords;
	// for every source, assigns the number of semi deserialized stream records that were scheduled for replay from that source
	private static HashMap<Integer, Integer> replayCounter;

	public static class MockFTLayer extends FTLayer {
		public HashMap<Long, Integer> sourceIdOfRecord;

		public MockFTLayer() {
			super();
			this.ackerTable = new MockAckerTable(this);
			this.sourceIdOfRecord = super.sourceIdOfRecord;
		}

		@Override
		public void ack(long sourceRecordId) {
			ackedRecordIds.add(sourceRecordId);
			super.ack(sourceRecordId);
		}

		@Override
		public void explicitFail(long sourceRecordId) {
			failedRecordIds.add(sourceRecordId);
			super.explicitFail(sourceRecordId);
		}

		@Override
		protected void collectToSourceSuccessiveTasks(int sourceId,
				SemiDeserializedStreamRecord sourceRecord) {
			replayedRecords.get(sourceId).add(sourceRecord);
		}

		public AbstractPersistenceLayer<Long, RecordWithHashCode> getPersistenceLayer() {
			return this.persistenceLayer;
		}

		public AckerTable getAckerTable() {
			return this.ackerTable;
		}

	}

	public static class MockAckerTable extends AckerTable implements Serializable {
		private static final long serialVersionUID = 1L;

		protected MockAckerTable(FTLayer ftLayer) {
			super(ftLayer);
		}

		public long getAckValue(long sourceRecordId) {
			return ackTable.get(sourceRecordId);
		}
	}

	// checking after adding a new record to the FTLayer
	private void checkAfterNewSourceRecord(MockFTLayer ftLayer, SemiDeserializedStreamRecord sdssr,
			int sourceId) {
		MockAckerTable ackerTable = (MockAckerTable) ftLayer.getAckerTable();
		AbstractPersistenceLayer<Long, RecordWithHashCode> persistenceLayer = ftLayer.getPersistenceLayer();
		RecordId persistenceRecordId = sdssr.getId();
		byte[] record = sdssr.getArray();
		int hashCode = sdssr.getHashCode();

		assertTrue(ackerTable.contains(persistenceRecordId.getSourceRecordId()));
		assertEquals(0L, ackerTable.getAckValue(persistenceRecordId.getSourceRecordId()));
		assertTrue(persistenceLayer.contains(persistenceRecordId.getSourceRecordId()));
		assertEquals(record, persistenceLayer.get(persistenceRecordId.getSourceRecordId()).getSerializedRecord());
		assertEquals(hashCode, persistenceLayer.get(persistenceRecordId.getSourceRecordId()).getHashCode());
		assertTrue(ftLayer.sourceIdOfRecord.containsKey(persistenceRecordId.getSourceRecordId()));
		assertEquals(sourceId,
				(int) ftLayer.sourceIdOfRecord.get(persistenceRecordId.getSourceRecordId()));
	}

	// checking after a new xorMessage arrives to the FTLayer
	// the ackValue should be updated before checking!
	private void checkAfterXor(MockFTLayer ftLayer, long ackValue, RecordId persistenceRecordId,
			boolean isFinished) {
		MockAckerTable ackerTable = (MockAckerTable) ftLayer.getAckerTable();

		if (isFinished) {
			assertEquals(0L, ackValue);
			assertFalse(ackerTable.contains(persistenceRecordId.getSourceRecordId()));
			assertEquals(persistenceRecordId.getSourceRecordId(), (long) ackedRecordIds.getLast());
		} else {
			assertNotEquals(0L, ackValue);
			assertTrue(ackerTable.contains(persistenceRecordId.getSourceRecordId()));
			assertEquals(ackValue, ackerTable.getAckValue(persistenceRecordId.getSourceRecordId()));
		}
	}

	// checking after a fail event (that causes the MockFTLayer.fail() to be called)
	// the replayCounter should be updated before checking!
	private void checkAfterFail(MockFTLayer ftLayer, SemiDeserializedStreamRecord sdssr,
			int sourceId) {
		MockAckerTable ackerTable = (MockAckerTable) ftLayer.getAckerTable();
		AbstractPersistenceLayer<Long, RecordWithHashCode> persistenceLayer = ftLayer.getPersistenceLayer();
		RecordId persistenceRecordId = sdssr.getId();
		byte[] record = sdssr.getArray();
		int hashCode = sdssr.getHashCode();

		assertEquals(persistenceRecordId.getSourceRecordId(), (long) failedRecordIds.getLast());
		ArrayDeque<SemiDeserializedStreamRecord> replayedList = replayedRecords.get(sourceId);
		assertEquals((int) replayCounter.get(sourceId), replayedList.size());
		RecordId newId = replayedList.getLast().getId();
		assertFalse(ackerTable.contains(persistenceRecordId.getSourceRecordId()));
		assertFalse(persistenceLayer.contains(persistenceRecordId.getSourceRecordId()));
		assertFalse(ftLayer.sourceIdOfRecord.containsKey(persistenceRecordId.getSourceRecordId()));
		assertTrue(ackerTable.contains(newId.getSourceRecordId()));
		assertEquals(0L, ackerTable.getAckValue(newId.getSourceRecordId()));
		assertTrue(persistenceLayer.contains(newId.getSourceRecordId()));
		assertEquals(record, persistenceLayer.get(newId.getSourceRecordId()).getSerializedRecord());
		assertEquals(hashCode, persistenceLayer.get(newId.getSourceRecordId()).getHashCode());
		assertTrue(ftLayer.sourceIdOfRecord.containsKey(newId.getSourceRecordId()));
		assertEquals(sourceId, (int) ftLayer.sourceIdOfRecord.get(newId.getSourceRecordId()));
	}

	@Test
	public void ftLayerTestWithoutFail() {

		// streams:
		// 1, 3 --> source1
		// 2 --> source2

		// job graph:
		// source1 --> flatMap3
		// source2 --> flatMap3
		// flatMap3 --> sink4

		ackedRecordIds = new ArrayDeque<Long>();
		failedRecordIds = new ArrayDeque<Long>();
		replayCounter = new HashMap<Integer, Integer>();
		replayCounter.put(0, 0);
		replayCounter.put(1, 0);
		replayedRecords = new HashMap<Integer, ArrayDeque<SemiDeserializedStreamRecord>>();
		replayedRecords.put(0, new ArrayDeque<SemiDeserializedStreamRecord>());
		replayedRecords.put(1, new ArrayDeque<SemiDeserializedStreamRecord>());

		MockFTLayer ftLayer = new MockFTLayer();

		long ackValue1 = 0L;
		long ackValue2 = 0L;
		long ackValue3 = 0L;

		// stream records are represented by byte arrays in the FTLayer
		final byte[] record1 = new byte[]{1};
		final byte[] record2 = new byte[]{2};
		final byte[] record3 = new byte[]{3};
		// even before the source forwards a record, a RecordId is created with the 
		// source record id and sent to the PersistenceLayer as part of a SemiDeserializedStreamRecord
		final RecordId idToPL1 = new RecordId(12L, 12L);
		final RecordId idToPL2 = new RecordId(53L, 53L);
		final RecordId idToPL3 = new RecordId(48L, 48L);
		SemiDeserializedStreamRecord sdssr1 = new SemiDeserializedStreamRecord(
				ByteBuffer.wrap(record1), 1, idToPL1);
		SemiDeserializedStreamRecord sdssr2 = new SemiDeserializedStreamRecord(
				ByteBuffer.wrap(record2), 2, idToPL2);
		SemiDeserializedStreamRecord sdssr3 = new SemiDeserializedStreamRecord(
				ByteBuffer.wrap(record3), 3, idToPL3);

		// when a new record is created, it gets a new RecordId
		// these RecordIds would be generated randomly by the topology
		final RecordId id1 = new RecordId(7L, 12L);
		final RecordId id2 = new RecordId(95L, 53L);
		final RecordId id3 = new RecordId(33L, 48L);
		final RecordId id11 = new RecordId(9L, 12L);
		final RecordId id12 = new RecordId(42L, 12L);
		final RecordId id21 = new RecordId(11L, 53L);

		// initialization of source records
		ftLayer.newSourceRecord(sdssr1, 0);
		checkAfterNewSourceRecord(ftLayer, sdssr1, 0);
		ftLayer.newSourceRecord(sdssr2, 1);
		checkAfterNewSourceRecord(ftLayer, sdssr2, 1);
		ftLayer.newSourceRecord(sdssr3, 0);
		checkAfterNewSourceRecord(ftLayer, sdssr3, 0);

		// source1 --> flatMap3, source2 --> flatMap3
		ftLayer.xor(id1);
		ackValue1 ^= id1.getCurrentRecordId();
		checkAfterXor(ftLayer, ackValue1, idToPL1, false);
		ftLayer.xor(id2);
		ackValue2 ^= id2.getCurrentRecordId();
		checkAfterXor(ftLayer, ackValue2, idToPL2, false);
		ftLayer.xor(id3);
		ackValue3 ^= id3.getCurrentRecordId();
		checkAfterXor(ftLayer, ackValue3, idToPL3, false);

		// flatMap3 --> sink4
		ftLayer.xor(id11);
		ackValue1 ^= id11.getCurrentRecordId();
		checkAfterXor(ftLayer, ackValue1, idToPL1, false);
		ftLayer.xor(id12);
		ackValue1 ^= id12.getCurrentRecordId();
		checkAfterXor(ftLayer, ackValue1, idToPL1, false);
		ftLayer.xor(id1);
		ackValue1 ^= id1.getCurrentRecordId();
		checkAfterXor(ftLayer, ackValue1, idToPL1, false);
		ftLayer.xor(id21);
		ackValue2 ^= id21.getCurrentRecordId();
		checkAfterXor(ftLayer, ackValue2, idToPL2, false);
		ftLayer.xor(id2);
		ackValue2 ^= id2.getCurrentRecordId();
		checkAfterXor(ftLayer, ackValue2, idToPL2, false);
		ftLayer.xor(id3);
		ackValue3 ^= id3.getCurrentRecordId();
		checkAfterXor(ftLayer, ackValue3, idToPL3, true);

		// sink4
		ftLayer.xor(id11);
		ackValue1 ^= id11.getCurrentRecordId();
		checkAfterXor(ftLayer, ackValue1, idToPL1, false);
		ftLayer.xor(id12);
		ackValue1 ^= id12.getCurrentRecordId();
		checkAfterXor(ftLayer, ackValue1, idToPL1, true);
		ftLayer.xor(id21);
		ackValue2 ^= id21.getCurrentRecordId();
		checkAfterXor(ftLayer, ackValue2, idToPL2, true);
	}

	@Test
	public void ftLayerTestWithFail() {

		// streams:
		// 1, 3 --> source1
		// 2 --> source2

		// job graph:
		// source1 --> flatMap3
		// source2 --> flatMap3
		// flatMap3 --> sink4

		ackedRecordIds = new ArrayDeque<Long>();
		failedRecordIds = new ArrayDeque<Long>();
		replayCounter = new HashMap<Integer, Integer>();
		replayCounter.put(0, 0);
		replayCounter.put(1, 0);
		replayedRecords = new HashMap<Integer, ArrayDeque<SemiDeserializedStreamRecord>>();
		replayedRecords.put(0, new ArrayDeque<SemiDeserializedStreamRecord>());
		replayedRecords.put(1, new ArrayDeque<SemiDeserializedStreamRecord>());

		MockFTLayer ftLayer = new MockFTLayer();

		// stream records are represented by byte arrays in the FTLayer
		final byte[] record1 = new byte[]{1};
		final byte[] record2 = new byte[]{2};
		final byte[] record3 = new byte[]{3};
		// even before the source forwards a record, a RecordId is created with the 
		// source record id and sent to the PersistenceLayer as part of a SemiDeserializedStreamRecord
		final RecordId idToPL1 = new RecordId(12L, 12L);
		final RecordId idToPL2 = new RecordId(53L, 53L);
		final RecordId idToPL3 = new RecordId(48L, 48L);
		SemiDeserializedStreamRecord sdssr1 = new SemiDeserializedStreamRecord(
				ByteBuffer.wrap(record1), 1, idToPL1);
		SemiDeserializedStreamRecord sdssr2 = new SemiDeserializedStreamRecord(
				ByteBuffer.wrap(record2), 2, idToPL2);
		SemiDeserializedStreamRecord sdssr3 = new SemiDeserializedStreamRecord(
				ByteBuffer.wrap(record3), 3, idToPL3);

		// when a new record is created, it gets a new RecordId
		// these RecordIds would be generated randomly by the topology
		final RecordId id1 = new RecordId(7L, 12L);
		final RecordId id3 = new RecordId(33L, 48L);
		final RecordId id11 = new RecordId(9L, 12L);

		// initialization of source records
		// source record 2 fails shortly after initialization
		ftLayer.newSourceRecord(sdssr1, 0);
		ftLayer.newSourceRecord(sdssr2, 1);
		ftLayer.newSourceRecord(sdssr3, 0);

		ftLayer.explicitFail(idToPL2.getSourceRecordId());
		replayCounter.put(1, replayCounter.get(1) + 1);
		checkAfterFail(ftLayer, sdssr2, 1);

		// source1 --> flatMap3, source2 --> flatMap3
		// source record 2 is missing because we do not interested in replaying it now
		ftLayer.xor(id1);
		ftLayer.xor(id3);

		// flatMap3 --> sink4
		// source record 1 fails during the invocation of the flatMap function
		// source record 3 fails without invoking flatMap on it
		ftLayer.xor(id11);
		ftLayer.explicitFail(id11.getSourceRecordId());
		replayCounter.put(0, replayCounter.get(0) + 1);
		checkAfterFail(ftLayer, sdssr1, 0);
		ftLayer.explicitFail(id3.getSourceRecordId());
		replayCounter.put(0, replayCounter.get(0) + 1);
		checkAfterFail(ftLayer, sdssr3, 0);

	}
}
