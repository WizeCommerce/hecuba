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

import com.wizecommerce.hecuba.HecubaConstants;
import com.wizecommerce.hecuba.util.ConfigUtils;
import me.prettyprint.cassandra.service.OperationType;
import me.prettyprint.hector.api.HConsistencyLevel;
import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class HectorConsistencyPolicyTest {

	@Test
	public void testCassandraConsistencyPolicy() {
		Configuration configuration = ConfigUtils.getInstance().getConfiguration();

		// The default configuration should be to set the consistency policies
		// to Read.ONE and Write.ONE
		HectorConsistencyPolicy policy = new HectorConsistencyPolicy("NoCF");
		assertEquals(HConsistencyLevel.ONE, policy.get(OperationType.READ));
		assertEquals(HConsistencyLevel.ONE, policy.get(OperationType.WRITE));

		// Now set explicit policies and test those.

		// Start with read properties.

		checkOperationConfigurationLoading(configuration,
										   HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".consistencypolicy.read",
										   OperationType.READ);
		checkOperationConfigurationLoading(configuration,
										   HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".consistencypolicy.write",
										   OperationType.WRITE);

	}

	private void checkOperationConfigurationLoading(Configuration configuration, String operationProperty,
													OperationType operationType) {
		HectorConsistencyPolicy policy;
		for (HConsistencyLevel consistencyLevel : HConsistencyLevel.values()) {
			configuration.setProperty(operationProperty, consistencyLevel.toString());
			policy = new HectorConsistencyPolicy("NoCF");
			assertEquals("Error handling consistency level " + consistencyLevel.toString() +
								 " within HectorConsistencyPolicy.", consistencyLevel, policy.get(operationType));
		}
	}
}
