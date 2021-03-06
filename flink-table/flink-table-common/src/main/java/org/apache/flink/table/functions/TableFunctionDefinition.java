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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.table.functions.FunctionDefinition.Type.TABLE_FUNCTION;

/**
 * The function definition of an user-defined table function.
 */
@PublicEvolving
public final class TableFunctionDefinition extends FunctionDefinition {

	private final TableFunction<?> tableFunction;
	private final TypeInformation<?> resultType;

	public TableFunctionDefinition(
			String name,
			TableFunction<?> tableFunction,
			TypeInformation<?> resultType) {
		super(name, TABLE_FUNCTION);
		this.tableFunction = Preconditions.checkNotNull(tableFunction);
		this.resultType = Preconditions.checkNotNull(resultType);
	}

	public TableFunction<?> getTableFunction() {
		return tableFunction;
	}

	public TypeInformation<?> getResultType() {
		return resultType;
	}
}
