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

import com.wizecommerce.hecuba.CassandraCounterTestBase;
import com.wizecommerce.hecuba.CassandraParamsBean;
import com.wizecommerce.hecuba.HecubaClientManager;
import me.prettyprint.cassandra.serializers.LongSerializer;

import java.io.IOException;

public class HectorBasedCassandraCounterTest extends CassandraCounterTestBase {

	public HectorBasedCassandraCounterTest() throws IOException {
		super(HectorBasedCassandraCounterTest.class.getName());
	}

	public HecubaClientManager<Long> getHecubaClientManager(String clusterName, String locationURL, String ports,
															String keyspace, String columnFamily) {
		return new HectorBasedHecubaClientManager<Long>(clusterName, locationURL, ports, keyspace, columnFamily,
				LongSerializer.get(), false);
	}

	public HecubaClientManager<Long> getHecubaClientManager(CassandraParamsBean paramsBean) {
		return new HectorBasedHecubaClientManager<Long>(paramsBean, LongSerializer.get(), false);
	}

}
