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

import com.wizecommerce.hecuba.astyanax.AstyanaxBasedHecubaClientManager;
import com.wizecommerce.hecuba.hector.HectorBasedHecubaClientManager;
import com.wizecommerce.hecuba.util.ConfigUtils;
import org.apache.log4j.Logger;

public class HecubaObjectFactory {
	private static final Logger log = Logger.getLogger(HecubaObjectFactory.class);
	private static final HecubaObjectFactory instance = new HecubaObjectFactory();

	private HecubaObjectFactory() {
	}

	public static HecubaObjectFactory getInstance() {
		return instance;
	}

	public HecubaClientManager<Long> getHecubaClientManagerWithLongKeys(CassandraParamsBean parameters,
																		HecubaConstants.CassandraClientImplementation cassandraManagerType) {
		if (cassandraManagerType == HecubaConstants.CassandraClientImplementation.ASTYANAX) {
			return new AstyanaxBasedHecubaClientManager<>(parameters,
															  com.netflix.astyanax.serializers.LongSerializer.get());
		} else {
			// Default.
			return new HectorBasedHecubaClientManager<>(parameters,
															me.prettyprint.cassandra.serializers.LongSerializer.get(),
															true);
		}
	}

	public HecubaClientManager<String> getHecubaClientManagerWithStringKeys(CassandraParamsBean parameters,
																			HecubaConstants.CassandraClientImplementation cassandraManagerType) {
		if (cassandraManagerType == HecubaConstants.CassandraClientImplementation.ASTYANAX) {
			return new AstyanaxBasedHecubaClientManager<String>(parameters,
																com.netflix.astyanax.serializers.StringSerializer
																								.get());
		} else {
			// Default.
			return new HectorBasedHecubaClientManager<String>(parameters,
															  me.prettyprint.cassandra.serializers.StringSerializer
																								  .get(), true);
		}
	}

	@SuppressWarnings("unchecked")
	public <K> HecubaClientManager<K> getHecubaClientManager(CassandraParamsBean parameters, Class<K> keyClass) {
		final String clientManagerName = ConfigUtils.getInstance().getConfiguration().getString(
				HecubaConstants.HECUBA_CASSANDRA_CLIENT_IMPLEMENTATION_MANAGER, "HECTOR").toUpperCase();
		final HecubaConstants.CassandraClientImplementation cassandraManagerType =
				HecubaConstants.CassandraClientImplementation.valueOf(clientManagerName);
		if (keyClass == String.class) {
			return (HecubaClientManager<K>) getHecubaClientManagerWithStringKeys(parameters, cassandraManagerType);
		} else {
			return (HecubaClientManager<K>) getHecubaClientManagerWithLongKeys(parameters, cassandraManagerType);
		}
	}

}
