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
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.api.java.typeutils.PojoTypeInfo.accessStringForField;

public final class PojoComparatorGenerator<T> {
	private static final Map<String, Class<TypeSerializer<?>>> generatedClasses = new HashMap<>();
	private static final String packageName = "org.apache.flink.api.java.typeutils.runtime.generated";
	private static long counter = 0; // TODO: atomic?

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
		Class<TypeSerializer<?>> comparatorClazz;
		StringBuilder keyBuilder = new StringBuilder();
		keyBuilder.append(type.getCanonicalName());
		for(Field f : keyFields) {
			keyBuilder.append(f.getName());
		}
		String key = keyBuilder.toString();
		if (generatedClasses.containsKey(key)) {
			comparatorClazz = generatedClasses.get(key);
		} else {
			final String className = type.getSimpleName() + "_GeneratedComparator" + Long.toString(counter++);
			try {
				generateCode(className);
				comparatorClazz = InstantiationUtil.compile(type.getClassLoader(), packageName + "." + className, code);
				generatedClasses.put(key, comparatorClazz);
			} catch (Exception e) {
				throw new RuntimeException("Unable to generate comparator: " + className, e);
			}
		}
		Constructor<?>[] ctors = comparatorClazz.getConstructors();
		assert ctors.length == 1;
		try {
			return (TypeComparator<T>) ctors[0].newInstance(new Object[]{comparators, serializer, type});
		} catch (Exception e) {
			throw new RuntimeException("Unable to instantiate comparator using: " + ctors[0].getName(), e);
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
		for (int i = 0; i < keyFields.length; ++i) {
			hashMembers.append(String.format(
				"code *= TupleComparatorBase.HASH_SALT[%d & 0x1F];\n" +
				"code += this.f%d.hash(((" + typeName + ")value)." + accessStringForField(keyFields[i]) +
					");\n",
				i, i));
		}
		StringBuilder setReference = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			setReference.append(String.format(
				"this.f%d.setReference(((" + typeName + ")toCompare)." + accessStringForField(keyFields[i]) + ");\n",
				i));
		}
		StringBuilder equalToReference = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			equalToReference.append(String.format(
				"if (!this.f%d.equalToReference(((" + typeName + ")candidate)." +
				accessStringForField(keyFields[i]) + ")) {\n" +
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
				"cmp = f%d.compare(((" + typeName + ")first)." + accessStringForField(keyFields[i]) + "," +
				"((" + typeName + ")second)." + accessStringForField(keyFields[i]) + ");\n" +
				"if (cmp != 0) {\n" +
					"return cmp;\n" +
				"}\n", i));
		}
		StringBuilder putNormalizedKeys = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			putNormalizedKeys.append(String.format("if (%d >= numLeadingNormalizableKeys || numBytes <= 0) break;\n" +
				"len = normalizedKeyLengths[%d];\n" +
				"len = numBytes >= len ? len : numBytes;\n" +
				"f%d.putNormalizedKey(((" + typeName + ")value)." + accessStringForField(keyFields[i]) +
				", target, offset, len);\n" +
				"numBytes -= len;\n" +
				"offset += len;", i, i, i));
		}
		StringBuilder extractKeys = new StringBuilder();
		for (int i = 0; i < comparators.length; ++i) {
			extractKeys.append(String.format(
				"localIndex += f%d.extractKeys(((" + typeName + ")record)." + accessStringForField(keyFields[i]) +
					", target, localIndex);\n", i));
		}
		Map<String, Object> root = new HashMap<>();
		root.put("packageName", packageName);
		root.put("className", className);
		root.put("members", members.toString().split("\n"));
		root.put("initMembers", initMembers.toString().split("\n"));
		root.put("cloneMembers", cloneMembers.toString().split("\n"));
		root.put("flatComparators", flatComparators.toString().split("\n"));
		root.put("hashMembers", hashMembers.toString().split("\n"));
		root.put("normalizableKeys", normalizableKeys.toString().split("\n"));
		root.put("setReference", setReference.toString().split("\n"));
		root.put("equalToReference", equalToReference.toString().split("\n"));
		root.put("compareToReference", compareToReference.toString().split("\n"));
		root.put("compareFields", compareFields.toString().split("\n"));
		root.put("putNormalizedKeys", putNormalizedKeys.toString().split("\n"));
		root.put("extractKeys", extractKeys.toString().split("\n"));
		try {
			code = InstantiationUtil.getCodeFromTemplate("PojoComparatorTemplate.ftl", root);
		} catch (IOException e) {
			throw new RuntimeException("Unable to read template.", e);
		}
	}
}
