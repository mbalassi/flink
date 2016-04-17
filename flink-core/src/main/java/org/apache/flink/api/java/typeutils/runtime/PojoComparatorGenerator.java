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

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.StringUtils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.PojoTypeInfo.accesStringForField;

public final class PojoComparatorGenerator<T> {
	private static final Map<Class<?>, Class<TypeSerializer<?>>> generatedClasses = new HashMap<>();
	private static final String packageName = "org.apache.flink.api.java.typeutils.runtime.generated";

	private transient Field[] keyFields;
	private final TypeComparator<Object>[] comparators;
	private TypeSerializer<T> serializer;
	private final Class<T> type;
	private String code;

	public PojoComparatorGenerator(Field[] keyFields, TypeComparator<?>[] comparators, TypeSerializer<T> serializer,
									Class<T>	type) {
		this.keyFields = keyFields;
		this.comparators = (TypeComparator<Object>[]) comparators;

		this.type = type;
		this.serializer = serializer;
	}

	public TypeComparator<T> createComparator() {
		final String className = type.getSimpleName() + "_GeneratedComparator";
		try {
			Class<TypeSerializer<?>> comparatorClazz;
			if (generatedClasses.containsKey(type)) {
				comparatorClazz = generatedClasses.get(type);
			} else {
				generateCode(className);
				comparatorClazz = InstantiationUtil.compile(type.getClassLoader(), packageName + "." + className, code);
				generatedClasses.put(type, comparatorClazz);
			}
			Constructor<?>[] ctors = comparatorClazz.getConstructors();
			assert ctors.length == 1;
			return (TypeComparator<T>) ctors[0].newInstance(new Object[]{comparators, serializer, type});
		}
		catch (Exception e) {
			throw new RuntimeException("Unable to generate comparator: " + className, e);
		}
	}


	private void generateCode(String className) {
		String typeName = type.getCanonicalName();
		StringBuilder members = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			members.append(String.format("final TypeComparator f%d;\n", i));
		}
		StringBuilder initMembers = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			initMembers.append(String.format("f%d = comparators[%d];\n", i, i));
		}
		StringBuilder normalizableKeys = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			normalizableKeys.append(String.format("if (f%d.supportsNormalizedKey()) {\n" +
				"	if (f%d.invertNormalizedKey() != inverted) break;\n" +
				"	nKeys++;\n" +
				"	final int len = f%d.getNormalizeKeyLen();\n" +
				"	this.normalizedKeyLengths[%d] = len;\n" +
				"	nKeyLen += len;\n" +
				"} else {\n" +
				"	break;\n" +
				"}\n", i, i, i, i));
		}
		StringBuilder cloneMembers = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			cloneMembers.append(String.format("f%d = toClone.f%d.duplicate();\n", i, i));
		}
		StringBuilder flatComparators = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			flatComparators.append(String.format(
				"if(f%d instanceof CompositeTypeComparator) {\n" +
				"	((CompositeTypeComparator)f%d).getFlatComparator(flatComparators);\n" +
				"} else {\n" +
				"	flatComparators.add(f%d);\n" +
				"}\n", i, i, i));
		}
		StringBuilder hashMembers = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			hashMembers.append(String.format(
				"code *= TupleComparatorBase.HASH_SALT[%d & 0x1F];\n" +
				"code += this.f%d.hash(((" + typeName + ")value)." + accesStringForField(keyFields[i]) +
					");\n",
				i, i));
		}
		StringBuilder setReference = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			setReference.append(String.format(
				"this.f%d.setReference(((" + typeName + ")toCompare)." + accesStringForField(keyFields[i]) + ");\n",
				i));
		}
		StringBuilder equalToReference = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			equalToReference.append(String.format(
				"if (!this.f%d.equalToReference(((" + typeName + ")candidate)." +
				accesStringForField(keyFields[i]) + ")) {\n" +
				"	return false;\n" +
				"}\n", i));
		}
		StringBuilder compareToReference = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			compareToReference.append(String.format(
				"cmp = this.f%d.compareToReference(other.f%d);\n" +
				"if (cmp != 0) {\n" +
				"	return cmp;\n" +
				"}\n", i, i));
		}
		StringBuilder compareFields = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			compareFields.append(String.format(
				"cmp = f%d.compare(((" + typeName + ")first)." + accesStringForField(keyFields[i]) + "," +
				"((" + typeName + ")second)." + accesStringForField(keyFields[i]) + ");\n" +
				"if (cmp != 0) {\n" +
					"return cmp;\n" +
				"}\n", i));
		}
		StringBuilder putNormalizedKeys = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			putNormalizedKeys.append(String.format("if (%d >= numLeadingNormalizableKeys || numBytes <= 0) break;\n" +
				"len = normalizedKeyLengths[%d];\n" +
				"len = numBytes >= len ? len : numBytes;\n" +
				"f%d.putNormalizedKey(((" + typeName + ")value)." + accesStringForField(keyFields[i]) +
				", target, offset, len);\n" +
				"numBytes -= len;\n" +
				"offset += len;", i, i, i));
		}
		StringBuilder extractKeys = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			extractKeys.append(String.format(
				"localIndex += f%d.extractKeys(((" + typeName + ")record)." + accesStringForField(keyFields[i]) +
					", target, localIndex);\n", i));
		}
		@SuppressWarnings("unchecked")
		Tuple2<String, StringBuilder>[] replacements = new Tuple2[]{new Tuple2<>
			("@MEMBERS@", members), new Tuple2<>("@INITMEMBERS@", initMembers), new Tuple2<>("@NORMALIZABLEKEYS@",
			normalizableKeys), new Tuple2<>("@CLONEMEMBERS@", cloneMembers), new Tuple2<>("@FLATCOMPARATORS@",
			flatComparators), new Tuple2<>("@HASHMEMBERS@", hashMembers), new Tuple2<>("@SETREFERENCE@", setReference)
			, new Tuple2<>("@EQUALTOREFERENCE@", equalToReference), new Tuple2<>("@COMPARETOREFERENCE@",
			compareToReference), new Tuple2<>("@COMPAREFIELDS@", compareFields), new Tuple2<>("@PUTNORMALIZEDKEYS@",
			putNormalizedKeys), new Tuple2<>("@EXTRACTKEYS@", extractKeys)};
		String classTemplate = getClassTemplate(className);
		code = StringUtils.replaceStrings(classTemplate, replacements);
	}

	private static String getClassTemplate(String className) {
		String imports =
			"package " + packageName + ";\n" +
			"import java.io.IOException;\n" +
			"import java.io.ObjectInputStream;\n" +
			"import java.io.ObjectOutputStream;\n" +
			"import java.util.List;\n" +
			"import org.apache.flink.api.common.typeutils.CompositeTypeComparator;\n" +
			"import org.apache.flink.api.common.typeutils.TypeComparator;\n" +
			"import org.apache.flink.api.common.typeutils.TypeSerializer;\n" +
			"import org.apache.flink.core.memory.DataInputView;\n" +
			"import org.apache.flink.core.memory.DataOutputView;\n" +
			"import org.apache.flink.core.memory.MemorySegment;\n" +
			"import org.apache.flink.types.NullKeyFieldException;\n" + "" +
			"import org.apache.flink.api.java.typeutils.runtime.TupleComparatorBase;\n" +
			"import org.apache.flink.util.InstantiationUtil;\n\n";

		return imports +
		"public final class " + className + " extends CompositeTypeComparator implements java.io.Serializable {\n" +
		"private static final long serialVersionUID = 1L;\n" +
		"private final int[] normalizedKeyLengths;\n" +
		"private final int numLeadingNormalizableKeys;\n" +
		"private final int normalizableKeyPrefixLen;\n" +
		"private final int numKeyFields;\n" +
		"private final boolean invertNormKey;\n" +
		"private TypeSerializer serializer;\n" +
		"private final Class type;\n" +
		"@MEMBERS@\n" +
		"public " + className + "(TypeComparator[] comparators, TypeSerializer serializer, Class type) {\n" +
		"	@INITMEMBERS@\n" +
		"	this.type = type;\n" +
		"	this.numKeyFields = comparators.length;\n" +
		"	this.serializer = serializer;\n" +
		"	this.normalizedKeyLengths = new int[numKeyFields];\n" +
		"	int nKeys = 0;\n" +
		"	int nKeyLen = 0;\n" +
		"	boolean inverted = f0.invertNormalizedKey();\n" +
		"	do {\n" +
		"		@NORMALIZABLEKEYS@\n" +
		"	} while (false);\n" +
		"	this.numLeadingNormalizableKeys = nKeys;\n" +
		"	this.normalizableKeyPrefixLen = nKeyLen;\n" +
		"	this.invertNormKey = inverted;\n" +
		"}\n" +
		"private " + className + "(" + className + " toClone) {\n" +
		"	@CLONEMEMBERS@\n" +
		"	this.normalizedKeyLengths = toClone.normalizedKeyLengths;\n" +
		"	this.numKeyFields = toClone.numKeyFields;\n" +
		"	this.numLeadingNormalizableKeys = toClone.numLeadingNormalizableKeys;\n" +
		"	this.normalizableKeyPrefixLen = toClone.normalizableKeyPrefixLen;\n" +
		"	this.invertNormKey = toClone.invertNormKey;\n" +
		"	this.type = toClone.type;\n" +
		"	try {\n" +
		"		this.serializer = (TypeSerializer) InstantiationUtil.deserializeObject(\n" +
		"			InstantiationUtil.serializeObject(toClone.serializer), Thread.currentThread().getContextClassLoader());\n" +
		"	} catch (IOException e) {\n" +
		"		throw new RuntimeException(\"Cannot copy serializer\", e);\n" +
		"	} catch (ClassNotFoundException e) {\n" +
		"		throw new RuntimeException(\"Cannot copy serializer\", e);\n" +
		"	}\n" +
		"}\n" +
		"@Override\n" +
		"public void getFlatComparator(List flatComparators) {\n" +
		"	@FLATCOMPARATORS@\n" +
		"}\n" +
		"@Override\n" +
		"public int hash(Object value) {\n" +
		"	int i = 0;\n" +
		"	int code = 0;\n" +
		"	@HASHMEMBERS@\n" +
		"	return code;\n" +
		"}\n" +
		"@Override\n" +
		"public void setReference(Object toCompare) {\n" +
		"	@SETREFERENCE@\n" +
		"}\n" +
		"@Override\n" +
		"public boolean equalToReference(Object candidate) {\n" +
		"	@EQUALTOREFERENCE@\n" +
		"	return true;\n" +
		"}\n" +
		"@Override\n" +
		"public int compareToReference(TypeComparator referencedComparator) {\n" +
		"	" + className + " other = (" + className + ") referencedComparator;\n" +
		"	int cmp;\n" +
		"	@COMPARETOREFERENCE@\n" +
		"	return 0;\n" +
		"}\n" +
		"@Override\n" +
		"public int compare(Object first, Object second) {\n" +
		"	int cmp;\n" +
		"	@COMPAREFIELDS@\n" +
		"	return 0;\n" +
		"}\n" +
		"@Override\n" +
		"public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {\n" +
		"	Object first = this.serializer.createInstance();\n" +
		"	Object second = this.serializer.createInstance();\n" +
		"	first = this.serializer.deserialize(first, firstSource);\n" +
		"	second = this.serializer.deserialize(second, secondSource);\n" +
		"	return this.compare(first, second);\n" +
		"}\n" +
		"@Override\n" +
		"public boolean supportsNormalizedKey() {\n" +
		"	return this.numLeadingNormalizableKeys > 0;\n" +
		"}\n" +
		"@Override\n" +
		"public int getNormalizeKeyLen() {\n" +
		"	return this.normalizableKeyPrefixLen;\n" +
		"}\n" +
		"@Override\n" +
		"public boolean isNormalizedKeyPrefixOnly(int keyBytes) {\n" +
		"	return this.numLeadingNormalizableKeys < this.numKeyFields ||\n" +
		"		this.normalizableKeyPrefixLen == Integer.MAX_VALUE ||\n" +
		"		this.normalizableKeyPrefixLen > keyBytes;\n" +
		"}\n" +
		"@Override\n" +
		"public void putNormalizedKey(Object value, MemorySegment target, int offset, int numBytes) {\n" +
		"	int len;\n" +
		"	do {\n" +
		"	@PUTNORMALIZEDKEYS@\n" +
		"	} while (false);\n" +
		"}\n" +
		"@Override\n" +
		"public boolean invertNormalizedKey() {\n" +
		"	return this.invertNormKey;\n" +
		"}\n" +
		"@Override\n" +
		"public boolean supportsSerializationWithKeyNormalization() {\n" +
		"	return false;\n" +
		"}\n" +
		"@Override\n" +
		"public void writeWithKeyNormalization(Object record, DataOutputView target) throws IOException {\n" +
		"	throw new UnsupportedOperationException();\n" +
		"}\n" +
		"@Override\n" +
		"public Object readWithKeyDenormalization(Object reuse, DataInputView source) throws IOException {\n" +
		"	throw new UnsupportedOperationException();\n" +
		"}\n" +
		"@Override\n" +
		"public " + className + " duplicate() {\n" +
		"	return new " + className + "(this);\n" +
		"}\n" +
		"@Override\n" +
		"public int extractKeys(Object record, Object[] target, int index) {\n" +
		"	int localIndex = index;\n" +
		"	@EXTRACTKEYS@\n" +
		"	return localIndex - index;\n" +
		"}\n" +
		"}";
	}
}
