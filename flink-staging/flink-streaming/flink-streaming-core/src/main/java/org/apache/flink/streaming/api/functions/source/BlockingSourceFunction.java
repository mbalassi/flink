package org.apache.flink.streaming.api.functions.source;

public abstract class BlockingSourceFunction<T> extends RichParallelSourceFunction<T>{

	public abstract void toggleBlock();
	
}
