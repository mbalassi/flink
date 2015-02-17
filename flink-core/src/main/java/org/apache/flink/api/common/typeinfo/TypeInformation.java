/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public abstract class TypeInformation<T> implements Serializable {
	
	public abstract boolean isBasicType();
	
	public abstract boolean isTupleType();
	
	public abstract int getArity();
	
	public abstract Class<T> getTypeClass();

	/**
	 * Returns the generic parameters of this type.
	 *
	 * @return The list of generic parameters. This list can be empty.
	 */
	public List<TypeInformation<?>> getGenericParameters() {
		// Return an empty list as the default implementation
		return new LinkedList<TypeInformation<?>>();
	}


	public abstract boolean isKeyType();
	
	public abstract TypeSerializer<T> createSerializer(ExecutionConfig config);
	
	/**
	 * @return The number of fields in this type, including its sub-fields (for compsite types) 
	 */
	public abstract int getTotalFields();

}
