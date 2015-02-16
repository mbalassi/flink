---
title: "Source fault tolerance mechanism"
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## Building the JobGraph with and without fault-tolarance, configuring fault-tolarance layer

FTLayerVertex is a special AbstractInvokable, a regular part of the streaming topology, which is in charge of the fault-tolerance. When the jobGraph is built, the fTLayerVertex is built with it. It is connected to every source and task vertices in the topology. Fault-tolerance can have two kinds of states: turned ON and turned OFF. FTLayerStatus.ON turns on the fault tolerance layer while FTLayerStatus.OFF turns it off completely. Currently, the status can be set in the streamGraph (it should be moved to the user-facing part of the api later) and based on this status, the fTLayerVertex is built in the streamingJobGraphGenerator.

In StreamGraph:
*   turning fault-tolerance ON/OFF for the whole runtime by setting ftStatus (FTStatus is an enum type with two possible values: ON, OFF)
*   differentiating between source and task vertices
*   turning off iteration & source successive co-tasks when fault-tolerance is on
*   setting parallelism to 1 when fault-tolerance is on
*   assigning KeySelectors to sources

In StreamingJobGraphGenerator:
*   creating fTLayerBuilder
*   creating ftLayerVertex, connecting it to sources and tasks
*   setting source successive tasks
*   turning off chaining sources when fault-tolerance is turned on

FTLayerBuilder is an interface that is in charge of building the ftLayerVertex: creating and configuring FTLayerVertex and wiring it into the topology. The ftLayerVertex will be the first output of every source and the first input of every task.
The NopFTLayerBuilder implementation does nothing.
The OpFTLayerbuilder builds an operating FTLayerVertex, and sets sourceSuccessives and partitioningStrategies in the ftLayerConfig (see the Partitioning subsection later). FTLayerConfig is the analogue of StreamConfig.

## Serialization

This is mostly unused in the moment because of difficulties with the chained invokables. It should be fixed later on.

The idea was the following:
Sending StreamRecords on the network requires a serialization-deserialization step that can be time consuming. When we send a record from a source to the ftLayerVertex, and later when we replay it to a source succesive task, we would need two serialization-deserialization step. It is unnecessary, since ftLayerVertex has nothing to do with the records except persisting and replaying them. The serialised record is as suitable for this purpose as the original one. So, it is better not to deserialize the inputs of the ftLayerVertex and there is no need to serialize its outputs either.
The records can be serialized in the source to a SemiDeserializedStreamRecord (SDSSR in short), which consists of the serialized object, its recordId and its hashCode (after key selection). The latter two informations are needed by the fault-tolerance logic.
We need two kinds of serialization in the source: one for the ftLayerVertex and one for the source successive tasks.
When ftLayerVertex gets a serialized record, it will not know its type or its length in bytes. To help ftLayer deserialize the record, the source must write its length in the resulted ByteBuffer.
On the other hand, when a successive task gets a record, it knows exactly what kind of record it is. It does not need the length information.
SemiDeserializedStreamRecordSerializer can be used in the first case and AsStreamRecordSerializer in the second case. AsStreamRecordSerializer can be also used in the ftLayerVertex for record-replaying.

Currently, we use StreamRecords in the sources and do the serialization step two times. In the direction of the ftLayerWertex, we have AsSemiDeserializedStreamRecordSerializer, while to source successive tasks we use the original StreamRecordSerializer. FtLayerVertex uses AsStreamRecordSerializer for replaying.

## Runtime components and functionalities of fault-tolerance besides FTLayerVertex 

### RecordId

Every record has a RecordId, which consists of two long fields: a source record id, which is the identifier of the source record from which the current record has been derived; and a current record id for the record itself. Both identifiers are random long values and supposed to be unique throughout the cluster.
Source vertices have sequential integer IDs (this is used in FTLayer to identify sources).
The IdentifiableStreamRecord interface covers "something with a RecordId". It is used in place of StreamRecord and SemideserializedStreamRecord where only RecordId manipulation is performed.

### The XORing method
Sources communicate with the ftLayerVertex by events (broadcastEvent() at sources and sendTaskEvent() at tasks). These events always transfers one RecordId.
FtLayerVertex locally communicates with an AckerTable, which stores a map of source record ids to ackValues (ackTable). The ackValues are long values, initially set to 0. When a RecordId arrives, AckerTable finds its source record id in the ackTable, and bitwise XORs the current record id to the ackValue. If ackerTable sees all current record ids that correspond to a source record id twice, when the source record is processed (but not earlier!), the ackValue will be 0L for this source record id (bitwise XOR is idempotent). In other words, a source record is processed if and only if the corresponding ackValue becomes 0 for the first time. There is a chance that the ackValue will become 0 before the source record is completely processed, even if the unique identifiers are truly unique (e.g. 1 XOR 2 XOR 3 = 0), but this probability is vanishingly small when ids generated random et al.

### From the tasks' viewpoint
Suppose that a given task T reads a record R from one of its inputs. First thing what T does is that it saves R's RecordId for later. This is handled by AbstractFTHandler's AnchorHandler interface.
After that, the userFunction is invoked on R, producing new records. These records are emitted to none, one or more tasks. Directed output is possible. StreamOutput is the final collector, that emits records to exactly one task. Every single emitted record gets a new RecordId: the source record id is set to R's source record id by the anchorHandler, and a new current record id is generated. Before the record is sent out, a XorEvent with the newly generated RecordId is sent to the ftLayerVertex.
At the end of the invoke, after the R was fully processed (all output records have been emitted), it sends a XorEvent with R's RecordId to the ftLayerVertex. This indicates that the record has been processed.
Sources do the same without sending the second XorEvent. In addition, as the initial step, sources send source records to the ftLayerVertex for persisting. This task is handled by the Persisiter interface. Three information is sent to ftLayerVertex: the stream object, the RecordId of the StreamRecord and an integer hashCode, which is used for partitioning the replayed records in the ftLayerVertex.

### From the records' viewpoint
The recordId of a record is sent to the ftLayerVertex and XORed into the ackerTable exactly two times: first in the collector, before the record is emitted by task A (where it was born) to task B, second, at the end of the invokation of task B, where it ends its lifecycle. The second XOR is often called "ack"-ing.
Xoring differs for source and task vertices. In sources, the acking is omitted, and the XOR messages are broadcasted as events to the ftLayerVertex. In tasks, acking is always performed and, because of the reversed direction of the record writers, XOR messages are sent backwards by the sendTaskEvent() method.

**AbstractFTHandler** - base interface for fault tolerance, contains all of the fault tolerance functionality outside the ftLayerVertex: persisting, xoring/acking, anchoring and failing
**FTHandler** - operating implementation of AbstractFTHandler. Calling the fail function fails the next RecordId that is sent out by the XorHandler.
**NonFTHandler** - non-operating implementation of AbstractFTHandler
**FTHandler** is initialized in StreamVertex and StreamSourceVertex based on the value of ftStatus.

### Persisting

**Persister** - source record persisting functionality
Before a source emits a new record into the topology, the record is persisted in an AbstractPersistenceStorage. During persisting, the record is sent to the ftLayerVertex through the network, and passed to the FTLayer. There, the serialized object wrapped in the record is persisted with its source record id, and hashcode. Furthermore, the id of the source where the record came from is stored. This source id will be used in replaying.

### Anchoring

**AnchorHandler** - anchoring functionality
When new records created in the topology, we give them a new record id. The source record id is inherited from the (first) record from which the new record was created. This record is called the anchor record. We store it' RecordId temporarily in the anchorRecordId field of the anchorHandler in order to be able to recall it later, when a new record id is generated.

### XORing

**XorHandler** - record id XORing/acking functionality
XorHandler sends xorEvents, which ftLayerVertex listens to. Xor Events can trigger failing if failFlag is set to true.

## The fault-tolerance logic in StreamVertex, StreamInvokable and StreamOutput

An AbstractFTHandler is created in every StreamVertex (and StreamSourceVertex). It must be different for sources and tasks. It is then passed to the outputHandler.
OutputHandler passes abstractFTHandler to the corresponding ChainableInvokable and StreamOutputs. It also wraps the outer collector in an FTCollectorWrapper which is the place where persisting of source records occurs, before the collect-calls (task vertices have got a non-operating Persister). The invoke -if it's the head of the chain, i.e. nextRecord != null- anchors the incoming record's RecordId, calls the user function (where collecting of out records takes place) and acks the record at the end. We cannot anchor records in the SourceInvokable. Instead, source records get their newly generated anchorRecordId in the SourceFTPersister.
The final collector is StreamOutput, that emits records directly to the network. There, the new records, produced by the user function call, gets new RecordId from the anchorHandler and this RecordId gets XORed the first time by the XorHandler.

## Core fault tolerance logic

### FTLayerVertex

FTLayerVertex is a special AbstractInvokable, which provides communication between the network and the ftLayer. It is connected to every source and task vertices in the topology. It persists all of the source records, tracks their status via the XORing mechanism and replays failed records to the sources. It reads records from the sources and passes them to the ftLayer, sets the failedRecordCollector and recordReplayer with the sourceSuccesiveTasks and partitioningStrategies. It also listens to XORmessages. When registering inputs, MultiRecordReader is used.

### MultiReaders

For every persisted record, the FTLayer need to store also which source it came from. In order to easily achieve this we would need to get the input number from a UnionReader (this way we don't need to send the source index with the record while persisting). MultiRecordReader returns the index of source (instead of a boolean) and -1 if there is no more input record.
The current solution could be optimized (by somehow avoiding the usage of a map for the inputs).

### FTLayer, AckerTable, AbstractPersistenceLayer, AbstractPersistenceStorage

FTLayer is one of the core fault-tolerance classes. It persists records in an AbstractPersistenceLayer, replays them to the appropriate source when necessary and XORs RecordIds to an AckerTable. AckerTable stores a map of source record ids into ack values. When its xor function is invoked, it XORs the current record id to the source record id's ack value. If the ack value becomes 0, the corresponding sourceId will be acked to the ftLayer and deleted from the persistenceLayer and ackerTable. PersistenceLayer is the base class of persisting. It stores a map of source record ids to records and hash codes in an AbstractPersistenceStorage, which represents the phisical storage system.

### Failing and replaying

There are two kinds of failing:
-explicit failing, which is used for testing purposes. Throwing a FailException in the userFunction will fail the record on which the userFunction was invoked. Explicit failing does not work in sources.
-failing by timeout: this logic is covered by TimeoutPersistenceLayer and RotatingHashMap. When a certain amount of time has passed and the record is still waiting in the RotatingHashMap, the TimeoutPersistenceLayer will fail it.

After a fail, FTLayer's fail method is invoked which cleans up the failed source record id, creates a new one and schedule it for replaying.
FailedRecordCollector will send the failed sourceRecord to the sourceSuccessive tasks.

### Timeout

The FTLayer timeout logic is set only in the TimeoutPersistenceLayer using a RotatingHashMap.
A RotatingHashMap stores data in buckets (HashMaps). The rotate() call opens a new bucket and calls a predefined ExpiredFunction to every element of the last (expired) bucket.
When the FTLayer recieves a record to persist, it's added to the TimeoutPersistenceLayer which puts it into a RotatingHashMap. TimeoutPersistenceLayer also starts a thread which rotates the map every n milliseconds. At every timeout, the records that timeout get replayed.
TimeoutPersistenceLayer can only be constructed with a given ExpiredFunction which is added to the RotatingHashMap. This way the ExpiredFunction can call the replay method of the FTLayer.

### Replay partitioning

While replaying records, ftLayerVertex should mirror the partitioning of the sources.
*   the (Stream)RecordWriters used for replaying should have the same type of partitioners
*   additionally, in case of FieldsPartitioner, replaying should mimic sources' key selection

We assume, that a sourceSuccessive task has the same partitioning strategies on its inputs. We map source successive tasks to (input) partitioning strategies in StreamGraph. This map makes its way to ftLayerVertex through the ftLayerConfig.
For every partitioner of the streaming api, there is a corresponding partitioner in ft.layer.partitioner, which does the same logic, except, for SemiDeserializedStreamRecord instead of StreamRecord (the logic for FieldsPartitioner is slightly different). All of these partitioners implement the ReplayPartitioner interface.
When initializing the outputs of ftLayer, we use the partitioning strategies to create the corresponding ReplayPartitioners (it is done by ReplayPartitionerFactory) and give them to the replaying (Stream)RecordWriters. Here, we use our same-partitioning-on-inputs assumption, because we have only one writer per task. Rework with more writers per task is in progress.

The logic is slightly different for FieldsPartitioner, because its channel selection depends on the record itself, but we see only the serialized record in ftLayerVertex. Thus, in order to mimic fields partitioning in ftLayerVertex, we must add plus information to the record: the hash code of the keySelector's value.
KeySelectors are assigned to sources in OpFTLayerBuilder's connectToFTLayerVertex method. It is the KeySelector of the sources outputs' FieldsPartitioners, if there are any, or a ConstantKeySelector otherwise. Here, we assume that the KeySelectors are the same on the fields-partitioned outputs of a source. It is because we prefer to send only one additional integer (hash code) per record to the ftLayerVertex. Rework with more hash codes per record is in progress.
We send the keySelector to StreamSourceVertex through StreamConfig, and give it to the serializer. The AsSemiDeserializedStreamRecordSerializer applies its keySelector to the records to be serialized, gets the hashCode of the key selector's value and writes it to the target outputView along with the record's stream object and RecordId. This "hash code" is then stored in the persistence layer and used in ReplayFieldsPartitioner in place of the keySelector.

## Packages and classes

### Source packages

org.apache.flink.streaming.api.ft.layer
org.apache.flink.streaming.api.ft.layer.collector
org.apache.flink.streaming.api.ft.layer.event
org.apache.flink.streaming.api.ft.layer.id
org.apache.flink.streaming.api.ft.layer.partitioner
org.apache.flink.streaming.api.ft.layer.runtime
org.apache.flink.streaming.api.ft.layer.serialization
org.apache.flink.streaming.api.ft.layer.util

### Test packages

org.apache.flink.streaming.api.ft

### Other

IdentifiableStreamRecord
MultiBufferReaderBase
MultiReaderIterator
MultiRecordReader
MultiSingleInputReaderIterator
MultiUnionReaderIterator
PersistencePartitioner
ConstantKeySelector
FTLayerBuilder
OpFTLayerBuilder
FTLayerConfig

### Modified (just major changes):

StreamGraph
StreamingJobGraphGenerator
StreamVertex
StreamSourceVertex
SourceInvokable
StreamInvokable
CoInvokable
InputHandler
OutputHandler
StreamOutput
StreamRecord

### Base classes & interfaces in source packages:

org.apache.flink.streaming.api.ft.layer
FTLayer
AckerTable
AbstractPersistenceLayer
AbstractPersistenceStorage
RecordReplayer
RotatingHashMap
TimeoutPersistenceLayer

org.apache.flink.streaming.api.ft.layer.collector
FailedRecordCollector
FTCollectorWrapper

org.apache.flink.streaming.api.ft.layer.event
XorEvent
XorEventListener

org.apache.flink.streaming.api.ft.layer.id
RecordId

org.apache.flink.streaming.api.ft.layer.partitioner
ReplayPartitioner & implementations
ReplayPartitionerFactory

org.apache.flink.streaming.api.ft.layer.runtime
AbstractFTHandler & implementations
AnchorHandler & implementations
Persister & implementations
XorHandler & implementations
FTLayerVertex
FTLayerConfig
FTRecordReplayer

org.apache.flink.streaming.api.ft.layer.serialization
SemiDeserializedStreamRecord
SemiDeserializedStreamRecordSerializer
AsStreamRecordSerializer
AsSemiDeserializedStreamRecordSerializer

org.apache.flink.streaming.api.ft.layer.util
ExpiredFunction

### Tests

org.apache.flink.streaming.api.ft
TypeSerializerTest
SerializationTest
FTLayerTest
FaultToleranceWithTopologyTest
RotatingHashMapTest
---

*This documentation is maintained by the contributors of the individual components.
We kindly ask anyone that adds and changes components to eventually provide a patch
or pull request that updates these documents as well.*

