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

package com.wizecommerce.hecuba;

import com.datastax.driver.core.DataType;
import com.wizecommerce.hecuba.astyanax.AstyanaxBasedHecubaClientManager;
import com.wizecommerce.hecuba.datastax.DataStaxBasedHecubaClientManager;
import com.wizecommerce.hecuba.hector.HectorBasedHecubaClientManager;
import com.wizecommerce.hecuba.util.ConfigUtils;

public class HecubaObjectFactory {
	private static final HecubaObjectFactory instance = new HecubaObjectFactory();

	private HecubaObjectFactory() {
	}

	public static HecubaObjectFactory getInstance() {
		return instance;
	}

	public HecubaClientManager<Long> getHecubaClientManagerWithLongKeys(CassandraParamsBean parameters, HecubaConstants.CassandraClientImplementation cassandraManagerType) {
		switch (cassandraManagerType) {
		case ASTYANAX:
			return new AstyanaxBasedHecubaClientManager<>(parameters, com.netflix.astyanax.serializers.LongSerializer.get());
		case HECTOR:
			return new HectorBasedHecubaClientManager<>(parameters, me.prettyprint.cassandra.serializers.LongSerializer.get());
		case DATASTAX:
			return new DataStaxBasedHecubaClientManager<>(parameters, DataType.bigint());
		default:
			throw new RuntimeException("Unhandled CassandraManagerType: " + cassandraManagerType);
		}
	}

	public HecubaClientManager<String> getHecubaClientManagerWithStringKeys(CassandraParamsBean parameters, HecubaConstants.CassandraClientImplementation cassandraManagerType) {
		switch (cassandraManagerType) {
		case ASTYANAX:
			return new AstyanaxBasedHecubaClientManager<String>(parameters, com.netflix.astyanax.serializers.StringSerializer.get());
		case HECTOR:
			return new HectorBasedHecubaClientManager<String>(parameters, me.prettyprint.cassandra.serializers.StringSerializer.get());
		case DATASTAX:
			return new DataStaxBasedHecubaClientManager<>(parameters, DataType.text());
		default:
			throw new RuntimeException("Unhandled CassandraManagerType: " + cassandraManagerType);
		}
	}

	@SuppressWarnings("unchecked")
	public <K> HecubaClientManager<K> getHecubaClientManager(CassandraParamsBean parameters, Class<K> keyClass) {
		final String clientManagerName = ConfigUtils.getInstance().getConfiguration().getString(HecubaConstants.HECUBA_CASSANDRA_CLIENT_IMPLEMENTATION_MANAGER, "HECTOR")
				.toUpperCase();
		final HecubaConstants.CassandraClientImplementation cassandraManagerType = HecubaConstants.CassandraClientImplementation.valueOf(clientManagerName);
		if (keyClass == String.class) {
			return (HecubaClientManager<K>) getHecubaClientManagerWithStringKeys(parameters, cassandraManagerType);
		} else {
			return (HecubaClientManager<K>) getHecubaClientManagerWithLongKeys(parameters, cassandraManagerType);
		}
	}

}
