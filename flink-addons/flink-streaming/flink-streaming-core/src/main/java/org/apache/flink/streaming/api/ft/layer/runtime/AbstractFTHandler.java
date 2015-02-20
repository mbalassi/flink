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

package org.apache.flink.streaming.api.ft.layer.runtime;

import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.ft.layer.id.RecordId;
import org.apache.flink.streaming.api.streamrecord.IdentifiableStreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

/**
 * Abstract class to wrap fault tolerance functionality with the runtime layer.
 *
 * @param <T>
 * 		Type of the record to persist
 */
public abstract class AbstractFTHandler<T> extends XorHandler implements
		Persister<T>, AnchorHandler {
	protected Persister<T> persister;
	protected AnchorHandler anchorHandler;
	protected XorHandler xorHandler;

	public AbstractFTHandler(Persister<T> persister, XorHandler xorHandler, AnchorHandler
			anchorHandler) {
		this.persister = persister;
		this.anchorHandler = anchorHandler;
		this.xorHandler = xorHandler;
	}

	public abstract void fail();

	@Override
	public void setAnchorRecord(IdentifiableStreamRecord anchorRecord) {
		if(anchorRecord != null) {
			anchorHandler.setAnchorRecord(anchorRecord);
		}
	}

	@Override
	public RecordId getAnchorRecord() {
		return anchorHandler.getAnchorRecord();
	}

	@Override
	public RecordId setOutRecordId(SerializationDelegate<? extends IdentifiableStreamRecord>
			outRecord) {
		return anchorHandler.setOutRecordId(outRecord);
	}

	@Override
	public void xor(IdentifiableStreamRecord record) {
		if(record != null) {
			xorHandler.xor(record);
		}
	}

	@Override
	public void persist(StreamRecord<T> record) {
		persister.persist(record);
	}

	@Override
	protected void emit(RecordId recordId) throws Exception {
		xorHandler.emit(recordId);
	}

	@Override
	protected void fail(RecordId recordId) throws Exception {
		xorHandler.fail(recordId);
	}

	public abstract Collector<T> wrap(Collector<T> collector);

	@Override
	public void close() {
		persister.close();
	}
}
