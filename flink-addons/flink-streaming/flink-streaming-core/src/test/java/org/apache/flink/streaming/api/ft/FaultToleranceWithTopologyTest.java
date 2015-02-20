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

package org.apache.flink.streaming.api.ft;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.jobgraph.AbstractJobVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.api.FTLayerBuilder;
import org.apache.flink.streaming.api.NOpFTLayerBuilder;
import org.apache.flink.streaming.api.OpFTLayerBuilder;
import org.apache.flink.streaming.api.StreamConfig;
import org.apache.flink.streaming.api.StreamGraph;
import org.apache.flink.streaming.api.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.ft.layer.FTLayer;
import org.apache.flink.streaming.api.ft.layer.runtime.FTLayerVertex;
import org.apache.flink.streaming.api.ft.layer.event.FailException;
import org.apache.flink.streaming.api.ft.layer.runtime.FTLayerConfig;
import org.apache.flink.streaming.api.ft.layer.id.RecordId;
import org.apache.flink.streaming.api.ft.layer.util.RecordWithHashCode;
import org.apache.flink.streaming.api.ft.layer.serialization.SemiDeserializedStreamRecord;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.util.TestStreamEnvironment;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// Tests the fault tolerance layer (acking, failing and replaying records) through a built up topology
public class FaultToleranceWithTopologyTest {

	//	private static final long MEMORYSIZE = 32;
	private static final int PARALLELISM = 1;
	private static final int SOURCE_PARALLELISM = 1;
	private static final long MEMORYSIZE = 32;

	// lists of records that tasks see during the job
	private static HashMap<String, ArrayList<Long>> intermediateResults = new HashMap<String, ArrayList<Long>>();
	// lists of records that comes out from the sinks
	private static HashMap<String, ArrayList<Long>> results = new HashMap<String, ArrayList<Long>>();
	// counts xor messages grouped by source record id.
	private static HashMap<Long, HashMap<Long, Integer>> xorMessagesReceived = new HashMap<Long, HashMap<Long, Integer>>();
	// after a xor message received, checks whether the corresponding source record id is acked
	// it is enough to check that the processing of every single source record is finished at the end and there is no activity after that
	private static HashMap<Long, ArrayList<Boolean>> isSourceRecordIdProcessed = new HashMap<Long, ArrayList<Boolean>>();
	// list of failed record ids
	private static ArrayList<Long> failedIds = new ArrayList<Long>();

	private static HashMap<Integer, ArrayList<Integer>> hashCodes = new HashMap<Integer, ArrayList<Integer>>();
	private static HashMap<Integer, ArrayList<Integer>> replayedHashCodes = new HashMap<Integer, ArrayList<Integer>>();

	public static class MockTestStreamEnvironment extends TestStreamEnvironment {

		public MockTestStreamEnvironment(int degreeOfParallelism, long memorySize) {
			super(degreeOfParallelism, memorySize);
			this.streamGraph = new MockStreamGraph();
		}
	}

	private static class MockStreamGraph extends StreamGraph {

		@Override
		public JobGraph getJobGraph(String jobGraphName) {

			this.jobName = jobGraphName;
			StreamingJobGraphGenerator optimizer = new MockStreamingJobGraphGenerator(this);

			return optimizer.createJobGraph(jobGraphName);
		}

	}

	private static class MockStreamingJobGraphGenerator extends StreamingJobGraphGenerator {

		public MockStreamingJobGraphGenerator(StreamGraph streamGraph) {
			super(streamGraph);
		}

		private void init() {
			this.streamVertices = new HashMap<String, AbstractJobVertex>();
			this.builtNodes = new HashSet<String>();
			this.chainedConfigs = new HashMap<String, Map<String, StreamConfig>>();
			this.vertexConfigs = new HashMap<String, StreamConfig>();
			this.chainedNames = new HashMap<String, String>();

			if (ftStatus == FTLayerBuilder.FTStatus.ON) {
				this.ftBuilder = new MockOpFTLayerBuilder(this);
			} else {
				this.ftBuilder = new NOpFTLayerBuilder();
			}
		}
	}

	private static class MockOpFTLayerBuilder extends OpFTLayerBuilder {
		private static final Logger LOG = LoggerFactory.getLogger(MockOpFTLayerBuilder.class);

		public MockOpFTLayerBuilder(StreamingJobGraphGenerator jobGraphGenerator) {
			super(jobGraphGenerator);
		}

		@Override
		public void createFTLayerVertex(JobGraph jobGraph, int parallelism) {
			String vertexName = "MockFTLayerVertex";
			Class<? extends AbstractInvokable> vertexClass = MockFTLayerVertex.class;
			ftLayerVertex = new AbstractJobVertex(vertexName);
			jobGraph.addVertex(ftLayerVertex);
			ftLayerVertex.setInvokableClass(vertexClass);
			ftLayerVertex.setParallelism(parallelism);
			if (LOG.isDebugEnabled()) {
				LOG.debug("FTLayer parallelism set: {} for {}", parallelism, vertexName);
			}
			FTLayerConfig config = new FTLayerConfig(ftLayerVertex.getConfiguration());
			config.setNumberOfSources(sourceVertices.size());
			config.setBufferTimeout(100L);
		}
	}


	public static class MockFTLayerVertex extends FTLayerVertex {

		public MockFTLayerVertex() {
			instanceID = numOfTasks;
			ftLayer = new MockFTLayer();
			numOfTasks++;
		}

	}

	// FTLayer is mocked in order to collect xor messages and failed ids into lists
	public static class MockFTLayer extends FTLayer {
		private final static Logger LOG = LoggerFactory.getLogger(MockFTLayer.class);

		public MockFTLayer() {
			super();
		}

		@Override
		public void ack(long sourceRecordId) {
			super.ack(sourceRecordId);
			isSourceRecordIdProcessed.get(sourceRecordId).add(true);
		}

		@Override
		public void explicitFail(long sourceRecordId) {
			if (ackerTable.contains(sourceRecordId)) {
				failedIds.add(sourceRecordId);
				failedSourceRecordIds.add(sourceRecordId);

				// find sourceRecord
				int sourceId = sourceIdOfRecord.get(sourceRecordId);
				RecordWithHashCode recordWithHashCode = persistenceLayer.get(sourceRecordId);

				// generate new sourceRecordId
				RecordId newSourceRecordId = RecordId.newSourceRecordId();
				long newId = newSourceRecordId.getSourceRecordId();

				// add new sourceRecord to PersistenceLayer
				sourceIdOfRecord.put(newId, sourceId);
				persistenceLayer.put(newId, recordWithHashCode);
				xorMessagesReceived.put(newId, new HashMap<Long, Integer>());
				isSourceRecordIdProcessed.put(newId, new ArrayList<Boolean>());

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

		@Override
		public void newSourceRecord(SemiDeserializedStreamRecord sourceRecord, int sourceId) {
			long sourceRecordId = sourceRecord.getId().getSourceRecordId();
			int hashCode = sourceRecord.getHashCode();
			byte[] serializedRecord = sourceRecord.getArray();
			RecordWithHashCode recordWithHashCode = new RecordWithHashCode(serializedRecord,
					hashCode);
			if (!ackerTable.contains(sourceRecordId)) {
				ackerTable.newSourceRecord(sourceRecordId);
				xorMessagesReceived.put(sourceRecordId, new HashMap<Long, Integer>());
				isSourceRecordIdProcessed.put(sourceRecordId, new ArrayList<Boolean>());
				hashCodes.get(sourceId).add(sourceRecord.getHashCode());
			}
			sourceIdOfRecord.put(sourceRecordId, sourceId);
			persistenceLayer.put(sourceRecordId, recordWithHashCode);
		}

		@Override
		public void xor(RecordId recordId) {

			long sourceRecordId = recordId.getSourceRecordId();
			if (!failedSourceRecordIds.contains(sourceRecordId)) {
				if (!ackerTable.contains(sourceRecordId)) {
					ackerTable.newSourceRecord(sourceRecordId);
					xorMessagesReceived.put(sourceRecordId, new HashMap<Long, Integer>());
					isSourceRecordIdProcessed.put(sourceRecordId, new ArrayList<Boolean>());
				}
				HashMap<Long, Integer> recordIdMap = xorMessagesReceived.get(sourceRecordId);
				if (recordIdMap.containsKey(recordId.getCurrentRecordId())) {
					int counter = recordIdMap.get(recordId.getCurrentRecordId());
					recordIdMap.put(recordId.getCurrentRecordId(), ++counter);

				} else {
					recordIdMap.put(recordId.getCurrentRecordId(), 1);
				}
				isSourceRecordIdProcessed.get(recordId.getSourceRecordId()).add(false);
				ackerTable.xor(sourceRecordId, recordId.getCurrentRecordId());
			}
		}

		@Override
		protected void collectToSourceSuccessiveTasks(int sourceId,
				SemiDeserializedStreamRecord sourceRecord) {
			super.collectToSourceSuccessiveTasks(sourceId, sourceRecord);
			replayedHashCodes.get(sourceId).add(sourceRecord.getHashCode());
		}

	}

	// fails a source record id if it is contained in recordsToFail
	private static void testFail(long value, ArrayList<Long> recordsToFail) throws FailException {
		if (recordsToFail.contains(value)) {
			recordsToFail.remove(value);
			throw new FailException();
		}
	}

	public static class MySource implements SourceFunction<Long> {
		private static final long serialVersionUID = 1L;

		private ArrayList<Long> values;
		private ArrayList<Long> recordsToFail;

		public MySource(ArrayList<Long> values, List<Long> recordsToFail) {
			this.values = values;
			this.recordsToFail = new ArrayList<Long>(recordsToFail);
		}

		public MySource(ArrayList<Long> values) {
			this.values = values;
			this.recordsToFail = new ArrayList<Long>();
		}

		@Override
		public void invoke(Collector<Long> collector) throws Exception {

			for (long i : values) {
				testFail(i, recordsToFail);
				collector.collect(i);
			}
		}
	}

	public static class MyMap implements MapFunction<Long, Long> {
		private static final long serialVersionUID = 1L;
		private String name;
		private ArrayList<Long> recordsToFail;

		public MyMap(String name, List<Long> recordsToFail) {
			this.recordsToFail = new ArrayList<Long>(recordsToFail);
			this.name = name;
		}

		public MyMap(String name) {
			this.recordsToFail = new ArrayList<Long>();
			this.name = name;
		}

		@Override
		public Long map(Long value) throws Exception {
			intermediateResults.get(name).add(value);
			testFail(value, recordsToFail);
			return value;
		}
	}

	private static final class MyFilter implements FilterFunction<Long> {
		private static final long serialVersionUID = 1L;
		private String name;
		private ArrayList<Long> recordsToFail;

		@SuppressWarnings("unused")
		public MyFilter(String name, List<Long> recordsToFail) {
			this.recordsToFail = new ArrayList<Long>(recordsToFail);
			this.name = name;
		}

		public MyFilter(String name) {
			this.recordsToFail = new ArrayList<Long>();
			this.name = name;
		}

		@Override
		public boolean filter(Long value) throws Exception {
			intermediateResults.get(name).add(value);
			testFail(value, recordsToFail);
			return value % 2 == 1;
		}
	}

	public static class MySink implements SinkFunction<Long> {
		private static final long serialVersionUID = 1L;
		private String name;
		private ArrayList<Long> recordsToFail;

		public MySink(String name, List<Long> recordsToFail) {
			this.recordsToFail = new ArrayList<Long>(recordsToFail);
			this.name = name;
		}

		public MySink(String name) {
			this.recordsToFail = new ArrayList<Long>();
			this.name = name;
		}

		@Override
		public void invoke(Long value) {
			intermediateResults.get(name).add(value);
			testFail(value, recordsToFail);
			results.get(name).add(value);
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void test() {
		hashCodes.put(0, new ArrayList<Integer>());
		hashCodes.put(1, new ArrayList<Integer>());
		replayedHashCodes.put(0, new ArrayList<Integer>());
		replayedHashCodes.put(1, new ArrayList<Integer>());

		// input to the sources
		HashMap<String, ArrayList<Long>> values = new HashMap<String, ArrayList<Long>>();
		// source records to fail
		// if there are more records with the same value, the first one with the given value will be failed
		HashMap<String, ArrayList<Long>> recordsToFail = new HashMap<String, ArrayList<Long>>();
		// expected records at tasks
		HashMap<String, ArrayList<Long>> intermediateExpected = new HashMap<String, ArrayList<Long>>();
		HashMap<String, ArrayList<Long>> expected = new HashMap<String, ArrayList<Long>>();
		// names of tasks and sinks
		HashMap<Integer, ArrayList<Integer>> expectedHashCodes = new HashMap<Integer, ArrayList<Integer>>();
		HashMap<Integer, ArrayList<Integer>> expectedReplayedHashCodes = new HashMap<Integer, ArrayList<Integer>>();
		HashSet<String> intermediateNames = new HashSet<String>();
		HashSet<String> sinkNames = new HashSet<String>();

		sinkNames.add("sink1");
		sinkNames.add("sink2");
		intermediateNames.add("map1");
		intermediateNames.add("map2");
		intermediateNames.add("filter1");
		intermediateNames.add("filter2");
		intermediateNames.addAll(sinkNames);

		values.put("source1", new ArrayList<Long>(Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L)));
		values.put("source2", new ArrayList<Long>(Arrays.asList(10L, 11L, 12L, 13L, 14L, 15L)));

		recordsToFail.put("source1", new ArrayList<Long>());
		recordsToFail.put("source2", new ArrayList<Long>());
		recordsToFail.put("map1", new ArrayList<Long>(Arrays.asList(3L, 4L)));
		recordsToFail.put("map2", new ArrayList<Long>());
		recordsToFail.put("filter1", new ArrayList<Long>());
		recordsToFail.put("filter2", new ArrayList<Long>());
		recordsToFail.put("sink1", new ArrayList<Long>(Arrays.asList(1L)));
		recordsToFail.put("sink2", new ArrayList<Long>());

		// 3L and 4L are failed in map1, that is the reason they expected to be duplicated
		// 1L is failed in sink1, hence it was re-sent to map1 too
		intermediateExpected.put("map1",
				new ArrayList<Long>(Arrays.asList(0L, 1L, 1L, 2L, 3L, 3L, 4L, 4L, 5L)));
		intermediateExpected.put("map2",
				new ArrayList<Long>(Arrays.asList(10L, 11L, 12L, 13L, 14L, 15L)));
		// 1L is failed in sink1, 3L and 4L are failed in map1
		intermediateExpected.put("filter1",
				new ArrayList<Long>(Arrays.asList(0L, 1L, 1L, 2L, 3L, 3L, 4L, 4L, 5L)));
		intermediateExpected.put("filter2",
				new ArrayList<Long>(Arrays.asList(10L, 11L, 12L, 13L, 14L, 15L)));
		// 1L is failed in sink1
		// 3L and 4L are failed in map1, so they don't reach sink1 before replaying
		intermediateExpected.put(
				"sink1",
				new ArrayList<Long>(Arrays.asList(0L, 1L, 1L, 2L, 3L, 4L, 5L, 10L, 11L, 12L, 13L,
						14L, 15L)));
		// 1L, 3L and 4L are replayed, but 4L is filtered out
		intermediateExpected.put("sink2",
				new ArrayList<Long>(Arrays.asList(1L, 1L, 3L, 3L, 5L, 11L, 13L, 15L)));

		// the hypothetical output of sink1 contains each source record once because 1L, 3L and 4L
		// fail before processing through sink1, and are replayed respectly
		expected.put(
				"sink1",
				new ArrayList<Long>(Arrays.asList(0L, 1L, 2L, 3L, 4L, 5L, 10L, 11L, 12L, 13L, 14L,
						15L)));
		expected.put("sink2", new ArrayList<Long>(Arrays.asList(1L, 1L, 3L, 3L, 5L, 11L, 13L, 15L)));

		expectedHashCodes.put(0, new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3, 4, 5)));
		expectedHashCodes.put(1, new ArrayList<Integer>(Arrays.asList(10, 11, 12, 13, 14, 15)));
		expectedReplayedHashCodes.put(0, new ArrayList<Integer>(Arrays.asList(1, 3, 4)));
		expectedReplayedHashCodes.put(1, new ArrayList<Integer>());

		for (String name : intermediateNames) {
			intermediateResults.put(name, new ArrayList<Long>());
		}
		for (String name : sinkNames) {
			results.put(name, new ArrayList<Long>());
		}

		// building the job graph
		final StreamExecutionEnvironment env = new MockTestStreamEnvironment(1, MEMORYSIZE);
		env.setDegreeOfParallelism(PARALLELISM);

		DataStream<Long> sourceStream1 = env.addSource(new MySource(values.get("source1")))
				.setParallelism(SOURCE_PARALLELISM);
		DataStream<Long> sourceStream2 = env.addSource(new MySource(values.get("source2")))
				.setParallelism(SOURCE_PARALLELISM);
		sourceStream1.filter(new MyFilter("filter1"))
				.merge(sourceStream2.filter(new MyFilter("filter2"))).addSink(new MySink("sink2"));
		sourceStream1.map(new MyMap("map1", recordsToFail.get("map1")))
				.merge(sourceStream2.map(new MyMap("map2")))
				.addSink(new MySink("sink1", recordsToFail.get("sink1")));

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

		for (String name : results.keySet()) {
			Collections.sort(results.get(name));
		}
		for (String name : intermediateResults.keySet()) {
			Collections.sort(intermediateResults.get(name));
		}
		for (String name : expected.keySet()) {
			Collections.sort(expected.get(name));
		}
		for (String name : intermediateExpected.keySet()) {
			Collections.sort(intermediateExpected.get(name));
		}
		for (Integer sourceId : hashCodes.keySet()) {
			Collections.sort(hashCodes.get(sourceId));
		}
		for (Integer sourceId : replayedHashCodes.keySet()) {
			Collections.sort(replayedHashCodes.get(sourceId));
		}
		for (Integer sourceId : expectedHashCodes.keySet()) {
			Collections.sort(expectedHashCodes.get(sourceId));
		}
		for (Integer sourceId : expectedReplayedHashCodes.keySet()) {
			Collections.sort(expectedReplayedHashCodes.get(sourceId));
		}

		for (String name : intermediateNames) {
			assertEquals(intermediateExpected.get(name), intermediateResults.get(name));
		}
		for (String name : sinkNames) {
			assertEquals(expected.get(name), results.get(name));
		}
		int expectedNumberOfSourceRecordIds = 0;
		for (String name : values.keySet()) {
			expectedNumberOfSourceRecordIds += values.get(name).size();
		}
		expectedNumberOfSourceRecordIds += failedIds.size();
		assertEquals(expectedNumberOfSourceRecordIds, xorMessagesReceived.keySet().size());
		for (long sourceRecordId : xorMessagesReceived.keySet()) {
			if (!failedIds.contains(sourceRecordId)) {
				HashMap<Long, Integer> recordIdMap = xorMessagesReceived.get(sourceRecordId);
				// checking if every record id xor'd twice
				for (long recordId : recordIdMap.keySet()) {
					assertEquals(2, (int) recordIdMap.get(recordId));
				}
			}
		}
		assertEquals(expectedNumberOfSourceRecordIds, isSourceRecordIdProcessed.keySet().size());
		for (long sourceRecordId : isSourceRecordIdProcessed.keySet()) {
			if (!failedIds.contains(sourceRecordId)) {
				ArrayList<Boolean> isIdProcessed = isSourceRecordIdProcessed.get(sourceRecordId);
				for (int i = 0; i < isIdProcessed.size() - 1; i++) {
					assertFalse(isIdProcessed.get(i));
				}
				assertTrue(isIdProcessed.get(isIdProcessed.size() - 1));
			}
		}

		assertEquals(expectedHashCodes, hashCodes);
		assertEquals(expectedReplayedHashCodes, replayedHashCodes);
	}

}
