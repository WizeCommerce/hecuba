package com.wizecommerce.hecuba.datastax;

import java.util.Map;

import com.datastax.driver.core.DataType;
import com.wizecommerce.hecuba.CassandraParamsBean;
import com.wizecommerce.hecuba.HecubaCassandraManagerTestBase;
import com.wizecommerce.hecuba.HecubaClientManager;

public class DataStaxBasedCassandraManagerTest extends HecubaCassandraManagerTestBase {

	@Override
	protected Map<String, Map<String, Object>> getData(String columnFamilyName) {
		return null;
	}

	@Override
	protected void tearDown() {

	}

	public HecubaClientManager<Long> getHecubaClientManager(CassandraParamsBean params) {
		return new DataStaxBasedHecubaClientManager<>(params, DataType.bigint());
	}

}
