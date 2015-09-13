package malom;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

/**
 * This will be the "value type" of the state graph in Gelly terminology.
 * Contains either a value or a count:
 *  - Count means the number of successors not yet determined to be a win.
 *    It also means that we haven't seen a loss successor.
 *    (If iterate has already run, then being a count means that this node has been determined to be a draw.)
 *  - value means that we know the final value of the node to be either a win or loss
 */
public class ValueCount implements Serializable {
	private static final long serialVersionUID = 1L;

	public Value value = Value.getNull();
	public short count = -1;

	public ValueCount() {}
	private ValueCount(Value value, short count) {
		this.value = value;
		this.count = count;
	}

	public ValueCount(ValueCount valueCount) {
		this.value = valueCount.value;
		this.count = valueCount.count;
	}

	public static ValueCount value(Value v){
		ValueCount r = new ValueCount();
		r.value = v;
		return r;
	}

	public static ValueCount count(int c){
		ValueCount r = new ValueCount();
		r.count = (short)c;
		return r;
	}

	public boolean isValue(){
		return !value.isNull();
	}

	public boolean isCount(){
		return count != -1;
	}

	@Override
	public boolean equals(Object o0) {
		if(o0 instanceof ValueCount) {
			ValueCount o = (ValueCount) o0;
			if(isValue()) {
				return o.isValue() && value.equals(o.value);
			} else if(isCount()){
				return o.isCount() && count == o.count;
			} else {
				assert false;
				return false;
			}
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return " ValueCount{" +
				"value=" + value +
				", count=" + count +
				'}';
	}

	static public class ValueCountSerializer extends TypeSerializer<ValueCount> {
		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<ValueCount> duplicate() {
			return this;
		}

		@Override
		public ValueCount createInstance() {
			return new ValueCount();
		}

		@Override
		public ValueCount copy(ValueCount from) {
			return new ValueCount(new Value(from.value), from.count);
		}

		@Override
		public ValueCount copy(ValueCount from, ValueCount reuse) {
			reuse.value.value = from.value.value;
			reuse.value.depth = from.value.depth;
			reuse.count = from.count;
			return reuse;
		}

		@Override
		public int getLength() {
			return 5;
		}

		@Override
		public void serialize(ValueCount record, DataOutputView target) throws IOException {
			target.writeByte(record.value.value);
			target.writeShort(record.value.depth);
			target.writeShort(record.count);
		}

		@Override
		public ValueCount deserialize(DataInputView source) throws IOException {
			byte valVal = source.readByte();
			short valDepth = source.readShort();
			short count = source.readShort();
			return new ValueCount(new Value(valVal, valDepth), count);
		}

		@Override
		public ValueCount deserialize(ValueCount reuse, DataInputView source) throws IOException {
			reuse.value.value = source.readByte();
			reuse.value.depth = source.readShort();
			reuse.count = source.readShort();
			return reuse;
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.write(source, getLength());
		}


		@Override
		public int hashCode() {
			return 42;
		}

		@Override
		public boolean equals(Object o) {
			return o instanceof ValueCountSerializer;
		}
	}
}
