/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *	 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ${packageName};
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public final class ${className} extends TypeSerializer {
    private static byte IS_NULL = 1;
    private static byte NO_SUBCLASS = 2;
    private static byte IS_SUBCLASS = 4;
    private static byte IS_TAGGED_SUBCLASS = 8;
    private int numFields;
    private ExecutionConfig executionConfig;
    private Map<Class, TypeSerializer> subclassSerializerCache;
    private final Map<Class, Integer> registeredClasses;
    private final TypeSerializer[] registeredSerializers;
    Class clazz;
    ${members}
    public ${className}(Class clazz, TypeSerializer[] serializerFields, ExecutionConfig e) {
        this.clazz = clazz;
        executionConfig = e;
        this.numFields = serializerFields.length;
        LinkedHashSet<Class> registeredPojoTypes = executionConfig.getRegisteredPojoTypes();
        subclassSerializerCache = new HashMap<Class, TypeSerializer>();
        List<Class> cleanedTaggedClasses = new ArrayList<Class>(registeredPojoTypes.size());
        for (Class registeredClass: registeredPojoTypes) {
            if (registeredClass.equals(clazz)) {
                continue;
            }
            if (!clazz.isAssignableFrom(registeredClass)) {
                continue;
            }
            cleanedTaggedClasses.add(registeredClass);
        }
        this.registeredClasses = new LinkedHashMap<Class, Integer>(cleanedTaggedClasses.size());
        registeredSerializers = new TypeSerializer[cleanedTaggedClasses.size()];
        int id = 0;
        for (Class registeredClass: cleanedTaggedClasses) {
            this.registeredClasses.put(registeredClass, id);
            TypeInformation typeInfo = TypeExtractor.createTypeInfo(registeredClass);
            registeredSerializers[id] = typeInfo.createSerializer(executionConfig);
            id++;
        }
        ${initMembers}
    }
    private TypeSerializer getSubclassSerializer(Class subclass) {
        TypeSerializer result = (TypeSerializer)subclassSerializerCache.get(subclass);
        if (result == null) {
            TypeInformation typeInfo = TypeExtractor.createTypeInfo(subclass);
            result = typeInfo.createSerializer(executionConfig);
            subclassSerializerCache.put(subclass, result);
        }
        return result;
    }
    public boolean isImmutableType() { return false; }
    public ${className} duplicate() {
        boolean stateful = false;
        TypeSerializer[] duplicateFieldSerializers = new TypeSerializer[numFields];
        ${duplicateSerializers}
        if (stateful) {
            return new ${className}(clazz, duplicateFieldSerializers, executionConfig);
        } else {
            return this;
        }
    }
    public ${typeName} createInstance() {
        if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
            return null;
        }
        try {
            ${typeName} t = (${typeName})clazz.newInstance();
            initializeFields(t);
            return t;
        }
        catch (Exception e) {
            throw new RuntimeException("Cannot instantiate class.", e);
        }
    }
    protected void initializeFields(${typeName} t) {
        ${createFields}
    }
    public ${typeName} copy(Object from) {
        if (from == null) return null;
        Class<?> actualType = from.getClass();
        ${typeName} target;
        if (actualType == clazz) {
            try {
                target = (${typeName}) from.getClass().newInstance();
            }
            catch (Throwable t) {
                throw new RuntimeException("Cannot instantiate class.", t);
            }
            ${copyFields}
            return target;
        } else {
            TypeSerializer subclassSerializer = getSubclassSerializer(actualType);
            return (${typeName})subclassSerializer.copy(from);
        }
    }
    public ${typeName} copy(Object from, Object reuse) {
        if (from == null) return null;
        Class actualType = from.getClass();
        if (actualType == clazz) {
            if (reuse == null || actualType != reuse.getClass()) {
                return copy(from);
            }
            ${reuseCopyFields}
            return (${typeName})reuse;
        } else {
            TypeSerializer subclassSerializer = getSubclassSerializer(actualType);
            return (${typeName})subclassSerializer.copy(from, reuse);
        }
    }
    public int getLength() {  return -1; } // TODO: make it smarter based on annotations?
    <#if isFinal == "true">
        @Override
        public void serialize(Object value, DataOutputView target) throws IOException {
        	if (value == null) {
        		target.writeByte(IS_NULL);
        		return;
        	}
        	target.writeByte(NO_SUBCLASS);
        	${serializeFields}
        }
        @Override
        public ${typeName} deserialize(DataInputView source) throws IOException {
        	int flags = source.readByte();
        	if((flags & IS_NULL) != 0) {
        		return null;
        	}
        	${typeName} target = null;
        	target = createInstance();
        	${deserializeFields}
        	return target;
        }
        @Override
        public ${typeName} deserialize(Object reuse, DataInputView source) throws IOException {
        	int flags = source.readByte();
        	if((flags & IS_NULL) != 0) {
        		return null;
        	}
        	if (reuse == null) {
        		reuse = createInstance();
        	}
        	${reuseDeserializeFields}
        	return (${typeName})reuse;
        }
        public void copy(DataInputView source, DataOutputView target) throws IOException {
        	int flags = source.readByte();
        	target.writeByte(flags);
        	if ((flags & IS_NULL) != 0) {
        		return;
        	}
        	${dataCopyFields}
        }
    </#if>
    <#if isFinal != "true">
        @Override
        public void serialize(Object value, DataOutputView target) throws IOException {
        	int flags = 0;
        	if (value == null) {
        		flags |= IS_NULL;
        		target.writeByte(flags);
        		return;
        	}
        	Integer subclassTag = -1;
        	Class actualClass = value.getClass();
        	TypeSerializer subclassSerializer = null;
        	if (clazz != actualClass) {
        		subclassTag = (Integer)registeredClasses.get(actualClass);
        		if (subclassTag != null) {
        			flags |= IS_TAGGED_SUBCLASS;
        			subclassSerializer = registeredSerializers[subclassTag.intValue()];
        		} else {
        			flags |= IS_SUBCLASS;
        			subclassSerializer = getSubclassSerializer(actualClass);
        		}
        	} else {
        		flags |= NO_SUBCLASS;
        	}
        	target.writeByte(flags);
        	if ((flags & IS_SUBCLASS) != 0) {
        		target.writeUTF(actualClass.getName());
        	} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {
        		target.writeByte(subclassTag);
        	}
        	if ((flags & NO_SUBCLASS) != 0) {
        		${serializeFields}
        	} else {
        		subclassSerializer.serialize(value, target);
        	}
        }
        @Override
        public ${typeName} deserialize(DataInputView source) throws IOException {
        	int flags = source.readByte();
        	if((flags & IS_NULL) != 0) {
        		return null;
        	}
        	Class actualSubclass = null;
        	TypeSerializer subclassSerializer = null;
        	${typeName} target = null;
        	if ((flags & IS_SUBCLASS) != 0) {
        		String subclassName = source.readUTF();
        		try {
        			actualSubclass = Class.forName(subclassName, true,
        										Thread.currentThread().getContextClassLoader());
        		} catch (ClassNotFoundException e) {
        			throw new RuntimeException("Cannot instantiate class.", e);
        		}
        		subclassSerializer = getSubclassSerializer(actualSubclass);
        		target = (${typeName}) subclassSerializer.createInstance();
        		initializeFields(target);
        	} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {
        		int subclassTag = source.readByte();
        		subclassSerializer = registeredSerializers[subclassTag];
        		target = (${typeName}) subclassSerializer.createInstance();
        		initializeFields(target);
        	} else {
        		target = createInstance();
        	}
        	if ((flags & NO_SUBCLASS) != 0) {
        		${deserializeFields}
        	} else {
        		target = (${typeName})subclassSerializer.deserialize(target, source);
        	}
        	return target;
        }
        @Override
        public ${typeName} deserialize(Object reuse, DataInputView source) throws IOException {
        	int flags = source.readByte();
        	if((flags & IS_NULL) != 0) {
        		return null;
        	}
        	Class subclass = null;
        	TypeSerializer subclassSerializer = null;
        	if ((flags & IS_SUBCLASS) != 0) {
        		String subclassName = source.readUTF();
        		try {
        			subclass = Class.forName(subclassName, true,
        										Thread.currentThread().getContextClassLoader());
        		} catch (ClassNotFoundException e) {
        			throw new RuntimeException("Cannot instantiate class.", e);
        		}
        		subclassSerializer = getSubclassSerializer(subclass);
        		if (reuse == null || subclass != reuse.getClass()) {
        			reuse = subclassSerializer.createInstance();
        			initializeFields((${typeName})reuse);
        		}
        	} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {
        		int subclassTag = source.readByte();
        		subclassSerializer = registeredSerializers[subclassTag];
        		if (reuse == null /* TOOD:|| subclassSerializer.clazz != reuse.getClass()*/) {
        			reuse = subclassSerializer.createInstance();
        			initializeFields((${typeName})reuse);
        		}
        	} else {
        		if (reuse == null || clazz != reuse.getClass()) {
        			reuse = createInstance();
        		}
        	}
        	if ((flags & NO_SUBCLASS) != 0) {
        		${reuseDeserializeFields}
        	} else {
        		reuse = (${typeName})subclassSerializer.deserialize(reuse, source);
        	}
        	return (${typeName})reuse;
        }
        public void copy(DataInputView source, DataOutputView target) throws IOException {
        	int flags = source.readByte();
        	target.writeByte(flags);
        	TypeSerializer subclassSerializer = null;
        	if ((flags & IS_NULL) != 0) {
        		return;
        	}
        	if ((flags & IS_SUBCLASS) != 0) {
        		String className = source.readUTF();
        		target.writeUTF(className);
        		try {
        			Class subclass = Class.forName(className, true, Thread.currentThread()
        				.getContextClassLoader());
        			subclassSerializer = getSubclassSerializer(subclass);
        		} catch (ClassNotFoundException e) {
        			throw new RuntimeException("Cannot instantiate class.", e);
        		}
        	} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {
        		int subclassTag = source.readByte();
        		target.writeByte(subclassTag);
        		subclassSerializer = registeredSerializers[subclassTag];
        	}
        	if ((flags & NO_SUBCLASS) != 0) {
        		${dataCopyFields}
        	} else {
        		subclassSerializer.copy(source, target);
        	}
        }
    </#if>
    public boolean equals(Object obj) {
        if (obj instanceof ${className}) {
            ${className} other = (${className})obj;
            return other.canEqual(this) && this.clazz == other.clazz && this.numFields == other.numFields
                    && ${memberEquals} && Arrays.equals(registeredSerializers, other.registeredSerializers) &&
					registeredClasses.equals(other.registeredClasses);
        } else {
            return false;
        }
    }
    public boolean canEqual(Object obj) { return obj instanceof ${className}; }
    public int hashCode() {
		return 31 * (31 * Arrays.hashCode(new TypeSerializer[]{${memberHash}}) +
		Arrays.hashCode(registeredSerializers)) + Objects.hash(clazz, numFields, registeredClasses);
    }
}
