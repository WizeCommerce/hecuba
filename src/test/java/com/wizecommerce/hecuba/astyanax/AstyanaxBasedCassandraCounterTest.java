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

package com.wizecommerce.hecuba.astyanax;

import com.netflix.astyanax.serializers.LongSerializer;
import com.wizecommerce.hecuba.CassandraCounterTestBase;
import com.wizecommerce.hecuba.CassandraParamsBean;
import com.wizecommerce.hecuba.HecubaClientManager;

import java.io.IOException;

public class AstyanaxBasedCassandraCounterTest extends CassandraCounterTestBase {

	public AstyanaxBasedCassandraCounterTest() throws IOException {
		super(AstyanaxBasedCassandraCounterTest.class.getName());
	}

	public HecubaClientManager<Long> getHecubaClientManager(String clusterName, String locationURL, String ports,
															String keyspace, String columnFamily) {
		return new AstyanaxBasedHecubaClientManager<Long>(clusterName, locationURL, ports, keyspace, columnFamily,
				LongSerializer.get());
	}

	public HecubaClientManager<Long> getHecubaClientManager(CassandraParamsBean paramsBean) {
		return new AstyanaxBasedHecubaClientManager<Long>(paramsBean, LongSerializer.get());
	}

}
