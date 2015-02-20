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

import static org.apache.flink.streaming.api.FTLayerBuilder.FTStatus;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.ft.layer.runtime.AnchorHandler;
import org.apache.flink.streaming.api.ft.layer.runtime.FTAnchorHandler;
import org.apache.flink.streaming.api.ft.layer.runtime.FTHandler;
import org.apache.flink.streaming.api.ft.layer.runtime.FTPersister;
import org.apache.flink.streaming.api.ft.layer.runtime.NonFTHandler;
import org.apache.flink.streaming.api.ft.layer.runtime.NonFTXorHandler;
import org.apache.flink.streaming.api.ft.layer.runtime.Persister;
import org.apache.flink.streaming.api.ft.layer.runtime.XorHandler;
import org.apache.flink.streaming.api.ft.layer.serialization.AsSemiDeserializedStreamRecordSerializer;
import org.apache.flink.streaming.api.invokable.SourceInvokable;
import org.apache.flink.streaming.api.streamrecord.StreamRecord;
import org.apache.flink.streaming.api.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.io.StreamRecordWriter;
import org.apache.flink.streaming.partitioner.PersistencePartitioner;
import org.apache.flink.util.MutableObjectIterator;

public class StreamSourceVertex<OUT> extends StreamVertex<OUT, OUT> {

	private SourceInvokable<OUT> sourceInvokable;

	public StreamSourceVertex() {
		super();
		sourceInvokable = null;
	}

//	@Override
//	public void initializeInvoke() {
//		// TODO initialize serializers of ftWriters
//	}

	@Override
	public void setInputsOutputs() {
		if (ftStatus == FTStatus.ON) {
			StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>> ftWriter = new
					StreamRecordWriter<SerializationDelegate<StreamRecord<OUT>>>
					(getNextWriter(), new PersistencePartitioner<OUT>(), 10);
			StreamRecordSerializer<OUT> serializer = configuration
					.getTypeSerializerOut1(userClassLoader);
			KeySelector<OUT, ?> keySelector = configuration.getKeySelector(userClassLoader);
			AsSemiDeserializedStreamRecordSerializer<OUT> semiDeserializedSerializer = new
					AsSemiDeserializedStreamRecordSerializer<OUT>(serializer, keySelector);
			AnchorHandler anchorHandler = new FTAnchorHandler();
			Persister<OUT> persister = new FTPersister<OUT>(anchorHandler, ftWriter,
					semiDeserializedSerializer);
			// TODO activate xor if events are working
//			XorHandler sourceXorHandler = new SourceFTXorHandler(ftWriter);
			XorHandler sourceXorHandler = new NonFTXorHandler();
			abstractFTHandler = new FTHandler<OUT>(persister, sourceXorHandler, anchorHandler);
		} else {
			abstractFTHandler = new NonFTHandler<OUT>();
		}
		userInvokable = sourceInvokable;
		outputHandler = new OutputHandler<OUT>(this, abstractFTHandler);
	}

//	@Override
//	protected void setInvokable() {
//		sourceInvokable = configuration.getUserInvokable(userClassLoader);
//		sourceInvokable.setup(this, abstractFTHandler);
//	}

//	@Override
//	public void invoke() throws Exception {
//		initializeInvoke();
//		// inputHandler = new InputHandler<OUT>(this);
//		outputHandler.invokeUserFunction("SOURCE", sourceInvokable);
//		abstractFTHandler.close();
//	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> MutableObjectIterator<X> getInput(int index) {
		return null;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <X> StreamRecordSerializer<X> getInputSerializer(int index) {
		return null;
	}

}
