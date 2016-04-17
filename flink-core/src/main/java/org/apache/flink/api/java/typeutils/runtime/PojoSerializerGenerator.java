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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.api.java.typeutils.PojoTypeInfo.accesStringForField;
import static org.apache.flink.api.java.typeutils.PojoTypeInfo.modifyStringForField;

public final class PojoSerializerGenerator<T> {
	private static final Map<Class<?>, Class<TypeSerializer<?>>> generatedClasses = new HashMap<>();
	private static final String packageName = "org.apache.flink.api.java.typeutils.runtime.generated";

	private final Class<T> clazz;
	private final Field[] refFields;
	private final TypeSerializer<?>[] fieldSerializers;
	private final ExecutionConfig config;
	private String code;

	public PojoSerializerGenerator(
		Class<T> clazz,
		TypeSerializer<?>[] fields,
		Field[] reflectiveFields,
		ExecutionConfig config) {
		this.clazz = checkNotNull(clazz);
		this.refFields = checkNotNull(reflectiveFields);
		this.fieldSerializers = checkNotNull(fields);
		this.config = checkNotNull(config);
		for (int i = 0; i < this.refFields.length; i++) {
			this.refFields[i].setAccessible(true);
		}
	}

	public TypeSerializer<T> createSerializer()  {
		final String className = clazz.getSimpleName() + "_GeneratedSerializer";
		try {
			Class<TypeSerializer<?>> serializerClazz;
			if (generatedClasses.containsKey(clazz)) {
				serializerClazz = generatedClasses.get(clazz);
			} else {
				generateCode(className);
				serializerClazz = InstantiationUtil.compile(clazz.getClassLoader(), packageName + "." + className, code);
				generatedClasses.put(clazz, serializerClazz);
			}
			Constructor<?>[] ctors = serializerClazz.getConstructors();
			assert ctors.length == 1;
			return (TypeSerializer<T>)ctors[0].newInstance(new Object[]{clazz, fieldSerializers, config});
		}
		catch (Exception e) {
			throw new RuntimeException("Unable to generate serializer: " + className, e);
		}
	}

	private void generateCode(String className) {
		assert fieldSerializers.length > 0;
		String typeName = clazz.getCanonicalName();
		StringBuilder members = new StringBuilder();
		for (int i = 0; i < fieldSerializers.length; ++i) {
			members.append(String.format("final TypeSerializer f%d;\n", i));
		}
		StringBuilder initMembers = new StringBuilder();
		for (int i = 0; i < fieldSerializers.length; ++i) {
			initMembers.append(String.format("f%d = serializerFields[%d];\n", i, i));
		}
		StringBuilder createFields = new StringBuilder();
		for (int i = 0; i < fieldSerializers.length; ++i) {
			createFields.append(String.format("((" + typeName + ")t)." + modifyStringForField(refFields[i],
				"f%d.createInstance()") + ";\n", i));
		}
		StringBuilder copyFields = new StringBuilder();
		copyFields.append("Object value;\n");
		for (int i = 0; i < fieldSerializers.length; ++i) {
			if (refFields[i].getType().isPrimitive()) {
				copyFields.append(String.format("((" + typeName + ")target)." + modifyStringForField(refFields[i],
					"f%d.copy(((" + typeName + ")from)." + accesStringForField(refFields[i])) + ");\n", i));
			} else {
				copyFields.append(String.format(
					"value = ((" + typeName + ")from)." + accesStringForField(refFields[i]) + ";\n" +
					"if (value != null) {\n" +
					"	((" + typeName + ")target)." + modifyStringForField(refFields[i], "f%d.copy(value)") + ";\n" +
					"} else {\n" +
					"	((" + typeName + ")target)." + modifyStringForField(refFields[i], "null") + ";\n" +
					"}\n", i));
			}
		}
		StringBuilder reuseCopyFields = new StringBuilder();
		reuseCopyFields.append("Object value;\n");
		reuseCopyFields.append("Object reuseValue;\n");
		reuseCopyFields.append("Object copy;\n");
		for (int i = 0; i < fieldSerializers.length; ++i) {
			if (refFields[i].getType().isPrimitive()) {
				reuseCopyFields.append(String.format("((" + typeName + ")reuse)." + modifyStringForField(refFields[i],
					"f%d.copy(((" + typeName + ")from)." + accesStringForField(refFields[i]) + ")") + ";\n", i));
			} else {
				reuseCopyFields.append(String.format(
					"value = ((" + typeName + ")from)." + accesStringForField(refFields[i]) + ";\n" +
					"if (value != null) {\n" +
					"	reuseValue = ((" + typeName + ")reuse)." + accesStringForField(refFields[i]) + ";\n" +
					"	if (reuseValue != null) {\n" +
					"		copy = f%d.copy(value, reuseValue);\n" +
					"	} else {\n" +
					"		copy = f%d.copy(value);\n" +
					"	}\n" +
					"	((" + typeName + ")reuse)." + modifyStringForField(refFields[i], "copy") + ";\n" +
					"} else {\n" +
					"	((" + typeName + ")reuse)." + modifyStringForField(refFields[i], "null") + ";\n" +
					"}\n", i, i));
			}
		}
		StringBuilder memberHash = new StringBuilder();
		for (int i = 0; i < fieldSerializers.length; ++i) {
			memberHash.append(String.format(" f%d,", i));
		}
		memberHash.deleteCharAt(memberHash.length() - 1);
		StringBuilder memberEquals = new StringBuilder();
		for (int i = 0; i < fieldSerializers.length; ++i) {
			memberEquals.append(String.format("Objects.equals(this.f%d, other.f%d) && ", i, i));
		}
		memberEquals.delete(memberEquals.length() - 3, memberEquals.length());
		StringBuilder serializeFields = new StringBuilder();
		serializeFields.append("Object o;\n");
		for (int i = 0; i < fieldSerializers.length; ++i) {
			if (refFields[i].getType().isPrimitive()) {
				serializeFields.append(String.format(
					"target.writeBoolean(false);\n" +
					"f%d.serialize(((" + typeName + ")value)." + accesStringForField(refFields[i]) + ", target);\n",
					i));
			} else {
				serializeFields.append(String.format(
					"o = ((" + typeName + ")value)." + accesStringForField(refFields[i]) + ";\n" +
					"if (o == null) {\n" +
					"	target.writeBoolean(true);\n" +
					"} else {\n" +
					"	target.writeBoolean(false);\n" +
					"	f%d.serialize(o, target);\n" +
					"}\n", i));
			}
		}
		StringBuilder deserializeFields = new StringBuilder();
		deserializeFields.append("boolean isNull;\n");
		for (int i = 0; i < fieldSerializers.length; ++i) {
			if (refFields[i].getType().isPrimitive()) {
				deserializeFields.append(String.format("source.readBoolean();\n" +
					"((" + typeName + ")target)." + modifyStringForField(refFields[i], "f%d.deserialize(source)") +
					";\n", i));
			} else {
				deserializeFields.append(String.format("isNull = source.readBoolean();\n" +
					"if (isNull) {\n" +
					"	((" + typeName + ")target)." + modifyStringForField(refFields[i], "null") + ";\n" +
					"} else {\n" +
					"	((" + typeName + ")target)." + modifyStringForField(refFields[i], "f%d.deserialize(source)") +
					";\n" +
					"}\n", i));
			}
		}
		StringBuilder reuseDeserializeFields = new StringBuilder();
		reuseDeserializeFields.append("boolean isNull;\n");
		reuseDeserializeFields.append("Object field;\n");
		reuseDeserializeFields.append("Object reuseField;\n");
		for (int i = 0; i < fieldSerializers.length; ++i) {
			if (refFields[i].getType().isPrimitive()) {
				reuseDeserializeFields .append(String.format("source.readBoolean();\n" +
					"((" + typeName + ")reuse)." + modifyStringForField(refFields[i], "f%d.deserialize(source)") +
					";\n", i));
			} else {
				reuseDeserializeFields .append(String.format("isNull = source.readBoolean();\n" +
					"if (isNull) {\n" +
					"	((" + typeName + ")reuse)." + modifyStringForField(refFields[i], "null") + ";\n" +
					"} else {\n" +
					"	reuseField = ((" + typeName + ")reuse)." + accesStringForField(refFields[i]) + ";\n" +
					"	if (reuseField != null) {\n" +
					"		field = f%d.deserialize(reuseField, source);\n" +
					"	} else {\n" +
					"		field = f%d.deserialize(source);\n" +
					"	}\n" +
					"	((" + typeName + ")reuse)." + modifyStringForField(refFields[i], "field") + ";\n" +
					"}\n", i, i, i, i, i));
			}
		}
		StringBuilder dataCopyFields = new StringBuilder();
		dataCopyFields.append("boolean isNull;\n");
		for (int i = 0; i < fieldSerializers.length; ++i) {
			if (refFields[i].getType().isPrimitive()) {
				dataCopyFields.append(String.format("source.readBoolean();\n" +
					"target.writeBoolean(false);\n" +
					"f%d.copy(source, target);\n", i));
			} else {
				dataCopyFields.append(String.format("isNull = source.readBoolean();\n" +
					"target.writeBoolean(isNull);\n" +
					"if (!isNull) {\n" +
					"	f%d.copy(source, target);\n" +
					"}\n", i));
			}
		}
		StringBuilder duplicateSerializers = new StringBuilder();
		for (int i = 0; i < fieldSerializers.length; ++i) {
			duplicateSerializers.append(String.format("duplicateFieldSerializers[%d] = f%d.duplicate();\n" +
				"if (duplicateFieldSerializers[%d] != f%d) {\n" +
				"	stateful = true;\n" +
				"}\n", i, i, i, i));
		}
		@SuppressWarnings("unchecked")
		Tuple2<String, StringBuilder>[] replacements = new Tuple2[]{new Tuple2<>("@MEMBERS@", members), new Tuple2<>
			("@INITMEMBERS@", initMembers), new Tuple2<>("@CREATEFIELDS@", createFields), new Tuple2<>("@COPYFIELDS@",
			copyFields), new Tuple2<>("@REUSECOPYFIELDS@", reuseCopyFields), new Tuple2<>("@MEMBERHASH@", memberHash),
			new Tuple2<>("@MEMBEREQUALS@", memberEquals), new Tuple2<>("@SERIALIZEFIELDS@", serializeFields), new
			Tuple2<>("@DESERIALIZEFIELDS@", deserializeFields), new Tuple2<>("@REUSEDESERIALIZEFIELDS@",
			reuseDeserializeFields), new Tuple2<>("@DATACOPYFIELDS@", dataCopyFields),
			new Tuple2<>("@DUPLICATESERIALIZERS@", duplicateSerializers)};
		String classTemplate = getClassTemplate(className, clazz.getCanonicalName(), Modifier.isFinal(clazz
			.getModifiers()));
		code = StringUtils.replaceStrings(classTemplate, replacements);
	}

	private static String getSerializeFunctionTemplate(boolean isFinal) {
		if (isFinal) {
			return "@Override\n" +
				"public void serialize(Object value, DataOutputView target) throws IOException {\n" +
				"	if (value == null) {\n" +
				"		target.writeByte(IS_NULL);\n" +
				"		return;\n" +
				"	}\n" +
				"	target.writeByte(NO_SUBCLASS);\n" +
				"	@SERIALIZEFIELDS@\n" +
				"}\n";
		} else {
			return "@Override\n" +
				"public void serialize(Object value, DataOutputView target) throws IOException {\n" +
				"	int flags = 0;\n" +
				"	if (value == null) {\n" +
				"		flags |= IS_NULL;\n" +
				"		target.writeByte(flags);\n" +
				"		return;\n" +
				"	}\n" +
				"	Integer subclassTag = -1;\n" +
				"	Class actualClass = value.getClass();\n" +
				"	TypeSerializer subclassSerializer = null;\n" +
				"	if (clazz != actualClass) {\n" +
				"		subclassTag = (Integer)registeredClasses.get(actualClass);\n" +
				"		if (subclassTag != null) {\n" +
				"			flags |= IS_TAGGED_SUBCLASS;\n" +
				"			subclassSerializer = registeredSerializers[subclassTag.intValue()];\n" +
				"		} else {\n" +
				"			flags |= IS_SUBCLASS;\n" +
				"			subclassSerializer = getSubclassSerializer(actualClass);\n" +
				"		}\n" +
				"	} else {\n" +
				"		flags |= NO_SUBCLASS;\n" +
				"	}\n" +
				"	target.writeByte(flags);\n" +
				"	if ((flags & IS_SUBCLASS) != 0) {\n" +
				"		target.writeUTF(actualClass.getName());\n" +
				"	} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {\n" +
				"		target.writeByte(subclassTag);\n" +
				"	}\n" +
				"	if ((flags & NO_SUBCLASS) != 0) {\n" +
				"		@SERIALIZEFIELDS@\n" +
				"	} else {\n" +
				"		subclassSerializer.serialize(value, target);\n" +
				"	}\n" +
				"}\n";
		}
	}

	private static String getDeserializeFunctionTemplate(String typeName, boolean isFinal) {
		if (isFinal) {
			return "@Override\n" +
				"public " + typeName + " deserialize(DataInputView source) throws IOException {\n" +
				"	int flags = source.readByte();\n" +
				"	if((flags & IS_NULL) != 0) {\n" +
				"		return null;\n" +
				"	}\n" +
				"	" + typeName + " target = null;\n" +
				"	target = createInstance();\n" +
				"	@DESERIALIZEFIELDS@\n" +
				"	return target;\n" +
				"}\n" +
				"@Override\n" +
				"public " + typeName + " deserialize(Object reuse, DataInputView source) throws IOException {\n" +
				"	int flags = source.readByte();\n" +
				"	if((flags & IS_NULL) != 0) {\n" +
				"		return null;\n" +
				"	}\n" +
				"	if (reuse == null) {\n" +
				"		reuse = createInstance();\n" +
				"	}\n" +
				"	@REUSEDESERIALIZEFIELDS@\n" +
				"	return (" + typeName + ")reuse;\n" +
				"}\n";
		} else {
			return "@Override\n" +
				"public " + typeName + " deserialize(DataInputView source) throws IOException {\n" +
				"	int flags = source.readByte();\n" +
				"	if((flags & IS_NULL) != 0) {\n" +
				"		return null;\n" +
				"	}\n" +
				"	Class actualSubclass = null;\n" +
				"	TypeSerializer subclassSerializer = null;\n" +
				"	" + typeName + " target = null;\n" +
				"	if ((flags & IS_SUBCLASS) != 0) {\n" +
				"		String subclassName = source.readUTF();\n" +
				"		try {\n" +
				"			actualSubclass = Class.forName(subclassName, true," +
				"										Thread.currentThread().getContextClassLoader());\n" +
				"		} catch (ClassNotFoundException e) {\n" +
				"			throw new RuntimeException(\"Cannot instantiate class.\", e);\n" +
				"		}\n" +
				"		subclassSerializer = getSubclassSerializer(actualSubclass);\n" +
				"		target = (" + typeName +") subclassSerializer.createInstance();\n" +
				"		initializeFields(target);\n" +
				"	} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {\n" +
				"		int subclassTag = source.readByte();\n" +
				"		subclassSerializer = registeredSerializers[subclassTag];\n" +
				"		target = (" + typeName +") subclassSerializer.createInstance();\n" +
				"		initializeFields(target);\n" +
				"	} else {\n" +
				"		target = createInstance();\n" +
				"	}\n" +
				"	if ((flags & NO_SUBCLASS) != 0) {\n" +
				"		@DESERIALIZEFIELDS@\n" +
				"	} else {\n" +
				"		target = (" + typeName +")subclassSerializer.deserialize(target, source);\n" +
				"	}\n" +
				"	return target;\n" +
				"}\n" +
				"@Override\n" +
				"public " + typeName + " deserialize(Object reuse, DataInputView source) throws IOException {\n" +
				"	int flags = source.readByte();\n" +
				"	if((flags & IS_NULL) != 0) {\n" +
				"		return null;\n" +
				"	}\n" +
				"	Class subclass = null;\n" +
				"	TypeSerializer subclassSerializer = null;\n" +
				"	if ((flags & IS_SUBCLASS) != 0) {\n" +
				"		String subclassName = source.readUTF();\n" +
				"		try {\n" +
				"			subclass = Class.forName(subclassName, true,\n" +
				"										Thread.currentThread().getContextClassLoader());\n" +
				"		} catch (ClassNotFoundException e) {\n" +
				"			throw new RuntimeException(\"Cannot instantiate class.\", e);\n" +
				"		}\n" +
				"		subclassSerializer = getSubclassSerializer(subclass);\n" +
				"		if (reuse == null || subclass != reuse.getClass()) {\n" +
				"			reuse = subclassSerializer.createInstance();\n" +
				"			initializeFields((" + typeName +")reuse);\n" +
				"		}\n" +
				"	} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {\n" +
				"		int subclassTag = source.readByte();\n" +
				"		subclassSerializer = registeredSerializers[subclassTag];\n" +
				"		if (reuse == null /* TOOD:|| subclassSerializer.clazz != reuse.getClass()*/) {\n" +
				"			reuse = subclassSerializer.createInstance();\n" +
				"			initializeFields((" + typeName +")reuse);\n" +
				"		}\n" +
				"	} else {\n" +
				"		if (reuse == null || clazz != reuse.getClass()) {\n" +
				"			reuse = createInstance();\n" +
				"		}\n" +
				"	}\n" +
				"	if ((flags & NO_SUBCLASS) != 0) {\n" +
				"		@REUSEDESERIALIZEFIELDS@\n" +
				"	} else {\n" +
				"		reuse = (" + typeName +")subclassSerializer.deserialize(reuse, source);\n" +
				"	}\n" +
				"	return (" + typeName + ")reuse;\n" +
				"}\n";
		}
	}

	private static String getSerializedCopy(boolean isFinal) {
		if (isFinal) {
			return "public void copy(DataInputView source, DataOutputView target) throws IOException {\n" +
				"	int flags = source.readByte();\n" +
				"	target.writeByte(flags);\n" +
				"	if ((flags & IS_NULL) != 0) {\n" +
				"		return;\n" +
				"	}\n" +
				"	@DATACOPYFIELDS@\n" +
				"}\n";
		} else {
			return "public void copy(DataInputView source, DataOutputView target) throws IOException {\n" +
				"	int flags = source.readByte();\n" +
				"	target.writeByte(flags);\n" +
				"	TypeSerializer subclassSerializer = null;\n" +
				"	if ((flags & IS_NULL) != 0) {\n" +
				"		return;\n" +
				"	}\n" +
				"	if ((flags & IS_SUBCLASS) != 0) {\n" +
				"		String className = source.readUTF();\n" +
				"		target.writeUTF(className);\n" +
				"		try {\n" +
				"			Class subclass = Class.forName(className, true, Thread.currentThread()\n" +
				"				.getContextClassLoader());\n" +
				"			subclassSerializer = getSubclassSerializer(subclass);\n" +
				"		} catch (ClassNotFoundException e) {\n" +
				"			throw new RuntimeException(\"Cannot instantiate class.\", e);\n" +
				"		}\n" +
				"	} else if ((flags & IS_TAGGED_SUBCLASS) != 0) {\n" +
				"		int subclassTag = source.readByte();\n" +
				"		target.writeByte(subclassTag);\n" +
				"		subclassSerializer = registeredSerializers[subclassTag];\n" +
				"	}\n" +
				"	if ((flags & NO_SUBCLASS) != 0) {\n" +
				"		@DATACOPYFIELDS@\n" +
				"	} else {\n" +
				"		subclassSerializer.copy(source, target);\n" +
				"	}\n" +
				"}\n";
		}
	}

	private static String getClassTemplate(String className, String typeName, boolean isFinal) {
		final String imports =
		"package " + packageName + ";\n" +
		"import java.io.IOException;\n" +
		"import java.io.ObjectInputStream;\n" +
		"import java.io.ObjectOutputStream;\n" +
		"import java.lang.reflect.Modifier;\n" +
		"import java.util.ArrayList;\n" +
		"import java.util.Arrays;\n" +
		"import java.util.HashMap;\n" +
		"import java.util.LinkedHashMap;\n" +
		"import java.util.LinkedHashSet;\n" +
		"import java.util.List;\n" +
		"import java.util.Map;\n" +
		"import java.util.Objects;\n" +
		"import org.apache.flink.api.common.ExecutionConfig;\n" +
		"import org.apache.flink.api.common.typeinfo.TypeInformation;\n" +
		"import org.apache.flink.api.common.typeutils.TypeSerializer;\n" +
		"import org.apache.flink.api.java.typeutils.TypeExtractor;\n" +
		"import org.apache.flink.core.memory.DataInputView;\n" +
		"import org.apache.flink.core.memory.DataOutputView;\n\n\n";

		return imports + "public final class " + className + " extends TypeSerializer {\n" +
			"private static byte IS_NULL = 1;\n" +
			"private static byte NO_SUBCLASS = 2;\n" +
			"private static byte IS_SUBCLASS = 4;\n" +
			"private static byte IS_TAGGED_SUBCLASS = 8;\n" +
			"private int numFields;\n" +
			"private ExecutionConfig executionConfig;\n" +
			"private Map<Class, TypeSerializer> subclassSerializerCache;\n" +
			"private final Map<Class, Integer> registeredClasses;\n" +
			"private final TypeSerializer[] registeredSerializers;\n" +
			"Class clazz;\n" +
			"@MEMBERS@\n" +
			"public " + className + "(Class clazz, TypeSerializer[] serializerFields, ExecutionConfig e) {\n " +
			"	this.clazz = clazz;\n" +
			"	executionConfig = e;\n" +
			"	this.numFields = serializerFields.length;\n" +
			"	LinkedHashSet<Class> registeredPojoTypes = executionConfig.getRegisteredPojoTypes();\n" +
			"	subclassSerializerCache = new HashMap<Class, TypeSerializer>();\n" +
			"	List<Class> cleanedTaggedClasses = new ArrayList<Class>(registeredPojoTypes.size());\n" +
			"	for (Class registeredClass: registeredPojoTypes) {\n" +
			"		if (registeredClass.equals(clazz)) {\n" +
			"			continue;\n" +
			"		}\n" +
			"		if (!clazz.isAssignableFrom(registeredClass)) {\n" +
			"			continue;\n" +
			"		}\n" +
			"		cleanedTaggedClasses.add(registeredClass);\n" +
			"	}\n" +
			"	this.registeredClasses = new LinkedHashMap<Class, Integer>(cleanedTaggedClasses.size());\n" +
			"	registeredSerializers = new TypeSerializer[cleanedTaggedClasses.size()];\n" +
			"	int id = 0;\n" +
			"	for (Class registeredClass: cleanedTaggedClasses) {\n" +
			"		this.registeredClasses.put(registeredClass, id);\n" +
			"		TypeInformation typeInfo = TypeExtractor.createTypeInfo(registeredClass);\n" +
			"		registeredSerializers[id] = typeInfo.createSerializer(executionConfig);\n" +
			"		id++;\n" +
			"	}\n" +
			"	@INITMEMBERS@\n" +
			"}\n"+
			"private TypeSerializer getSubclassSerializer(Class subclass) {\n" +
			"	TypeSerializer result = (TypeSerializer)subclassSerializerCache.get(subclass);\n" +
			"	if (result == null) {\n" +
			"		TypeInformation typeInfo = TypeExtractor.createTypeInfo(subclass);\n" +
			"		result = typeInfo.createSerializer(executionConfig);\n" +
			"		subclassSerializerCache.put(subclass, result);\n" +
			"	}\n" +
			"	return result;\n" +
			"}\n" +
			"public boolean isImmutableType() { return false; }\n" +
			"public " + className + " duplicate() {\n" +
			"	boolean stateful = false;\n" +
			"	TypeSerializer[] duplicateFieldSerializers = new TypeSerializer[numFields];\n" +
			"	@DUPLICATESERIALIZERS@\n" +
			"	if (stateful) {\n" +
			"		return new " + className + "(clazz, duplicateFieldSerializers, executionConfig);\n" +
			"	} else {\n" +
			"		return this;\n" +
			"	}\n" +
			"}\n" +
			"public " + typeName + " createInstance() {\n" +
			"	if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {\n" +
			"		return null;\n" +
			"	}\n" +
			"	try {\n" +
			"		" + typeName + " t = (" + typeName + ")clazz.newInstance();\n" +
			"		initializeFields(t);\n" +
			"		return t;\n" +
			"	}\n " +
			"	catch (Exception e) {\n" +
			"		throw new RuntimeException(\"Cannot instantiate class.\", e);\n" +
			"	}\n" +
			"}\n" +
			"protected void initializeFields(" + typeName + " t) {\n" +
			"	@CREATEFIELDS@\n" +
			"}\n" +
			"public " + typeName + " copy(Object from) {\n" +
			"	if (from == null) return null;\n" +
			"	Class<?> actualType = from.getClass();\n" +
			"	" + typeName + " target;\n" +
			"	if (actualType == clazz) {\n" +
			"		try {\n" +
			"			target = (" + typeName + ") from.getClass().newInstance();\n" +
			"		}\n" +
			"		catch (Throwable t) {\n" +
			"			throw new RuntimeException(\"Cannot instantiate class.\", t);\n" +
			"		}\n" +
			"		@COPYFIELDS@\n" +
			"		return target;\n" +
			"	} else {\n" +
			"		TypeSerializer subclassSerializer = getSubclassSerializer(actualType);\n" +
			"		return (" + typeName + ")subclassSerializer.copy(from);\n" +
			"	}\n" +
			"}\n" +
			"public " + typeName + " copy(Object from, Object reuse) {\n" +
			"	if (from == null) return null;\n" +
			"	Class actualType = from.getClass();\n" +
			"	if (actualType == clazz) {\n" +
			"		if (reuse == null || actualType != reuse.getClass()) {\n" +
			"			return copy(from);\n" +
			"		}\n" +
			"		@REUSECOPYFIELDS@\n" +
			"		return (" + typeName + ")reuse;\n" +
			"	} else {\n" +
			"		TypeSerializer subclassSerializer = getSubclassSerializer(actualType);\n" +
			"		return (" + typeName + ")subclassSerializer.copy(from, reuse);\n" +
			"	}\n" +
			"}\n" +
			"public int getLength() {\n  return -1; \n}\n" + // TODO: make it smarter based on annotations?
			getSerializeFunctionTemplate(isFinal) +
			getDeserializeFunctionTemplate(typeName, isFinal) +
			getSerializedCopy(isFinal) +
			"public boolean equals(Object obj) {\n" +
			"	if (obj instanceof " + className + ") {\n" +
			"		" + className + " other = (" + className + ")obj;\n" +
			"		return other.canEqual(this) && this.clazz == other.clazz && this.numFields == other.numFields\n" +
			"				&& @MEMBEREQUALS@;\n" +
			"	} else {\n" +
			"		return false;\n" +
			"	}\n" +
			"}\n" +
			"public boolean canEqual(Object obj) { return obj instanceof " + className + "; }\n" +
			"public int hashCode() {\n" +
			"	return Objects.hash(clazz, numFields, @MEMBERHASH@);\n" +
			"}\n" +
			"}\n";
	}
}

