/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wizecommerce.hecuba.hector;

import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.HConsistencyLevel;

import org.apache.commons.lang3.StringUtils;

import com.wizecommerce.hecuba.HecubaConstants;
import com.wizecommerce.hecuba.util.ConfigUtils;

public class HectorConsistencyPolicy implements ConsistencyLevelPolicy {

	private HConsistencyLevel readConsistency;
	private HConsistencyLevel writeConsistency;

	public HectorConsistencyPolicy(String columnFamilyName) {
		readConsistency = configureConsistencyLevel(columnFamilyName, "read");
		writeConsistency = configureConsistencyLevel(columnFamilyName, "write");
	}

	private HConsistencyLevel configureConsistencyLevel(String columnFamilyName, String operation) {
		// first get the default consistency policy.
		String[] consistencyPolicyProperties = HecubaConstants.getConsistencyPolicyProperties(columnFamilyName, operation);
		String consistencyParameter = "ONE";
		for (String property : consistencyPolicyProperties) {
			consistencyParameter = ConfigUtils.getInstance().getConfiguration().getString(property, consistencyParameter);
		}

		return StringUtils.isEmpty(consistencyParameter) ? HConsistencyLevel.ONE : HConsistencyLevel.valueOf(consistencyParameter);
	}

	@Override
	public HConsistencyLevel get(OperationType operationType) {
		switch (operationType) {
		case READ:
			return readConsistency;
		case WRITE:
			return writeConsistency;
		default:
			return HConsistencyLevel.ONE; // Just in Case
		}
	}

	@Override
	public HConsistencyLevel get(OperationType operationType, String colFamilyName) {
		// Our consistency level will not change depending on the column family.
		// So call the above method in here as well.
		return get(operationType);
	}
}
