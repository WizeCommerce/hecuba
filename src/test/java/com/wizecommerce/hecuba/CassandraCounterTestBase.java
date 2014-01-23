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

import com.wizecommerce.hecuba.util.CassandraTestBase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

/**
 * 
 * NOTE: 
 * All counters should have CounterColumnType column value type. Therefore the map returned in 
 * getColumnValueTypeOverrides() must include all methods names that tests counters.
 * Also, update the list returned in getSecondaryIndexExcludeList() appropriately.
 *
 */
public abstract class CassandraCounterTestBase extends CassandraTestBase {

	protected CassandraCounterTestBase(String className) throws IOException {
		super(className);
	}

	@Test
	public void testCounterUpdate() {
		String columnFamily = "testCounterUpdate";

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);

		//Update counter with value 5
		cassandraManager.updateCounter(1122L, "test_column_0", 5L);
		// Now the value should be 5
		assertEquals(new Long(5), cassandraManager.getCounterValue(1122L, "test_column_0"));
		
		// Update counter with value 1
		cassandraManager.updateCounter(1234L, "test_column_1", 1L);
		// Since there was no such counter earlier the value should be 1
		assertEquals(new Long(1), cassandraManager.getCounterValue(1234L, "test_column_1"));
		// Update counter with value 2
		cassandraManager.updateCounter(1234L, "test_column_1", 2L);
		// Now the value should be 3
		assertEquals(new Long(3), cassandraManager.getCounterValue(1234L, "test_column_1"));
		// Update counter with -1
		cassandraManager.updateCounter(1234L, "test_column_1", -1L);
		// Now the value should be 2
		assertEquals(new Long(2), cassandraManager.getCounterValue(1234L, "test_column_1"));
		//Update counter with 0
		cassandraManager.updateCounter(1234L, "test_column_1", 0L);
		//Now the value should still be 2
		assertEquals(new Long(2), cassandraManager.getCounterValue(1234L, "test_column_1"));
		
		//Update another counter with 0
		cassandraManager.updateCounter(1111L, "test_column_2", 0L);
		// Now the value should be 0
		assertEquals(new Long(0), cassandraManager.getCounterValue(1111L, "test_column_2"));
	}

	@Test
	public void testDeleteCounterColumn() {
		String columnFamily = "testDeleteCounterColumn";

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);
		// Set a counter
		cassandraManager.updateCounter(1234L, "test_column_1", 1L);
		// Check the value
		assertTrue(1 == cassandraManager.getCounterValue(1234L, "test_column_1"));

		// Delete the counter
		cassandraManager.deleteColumn(1234L, "test_column_1");
		//Now the value should be 0
		assertEquals(new Long(0), cassandraManager.getCounterValue(1234L, "test_column_1"));
	}

	@Test
	public void testReadNonExistentCounterColumn() {
		String columnFamily = "testReadNonExistentCounterColumn";

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);
		
		//Reading a counter that does not exist
		assertEquals(new Long(0), cassandraManager.getCounterValue(1234L, "test_column_1"));
	}
	
	@Test
	public void testIncrementCounter() {
		String columnFamily = "testIncrementCounter";

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);
		
		//Created a new counter with the starting value 10
		cassandraManager.updateCounter(1111L, "test_column_0", 10);
		//Value must be 10
		assertEquals(new Long(10), cassandraManager.getCounterValue(1111L, "test_column_0"));
		//Increment counter
		cassandraManager.incrementCounter(1111L, "test_column_0");
		//Value must be 11
		assertEquals(new Long(11), cassandraManager.getCounterValue(1111L, "test_column_0"));
		
		//Increment a counter that does not exist
		cassandraManager.incrementCounter(1234L, "test_column_1");
		//Value should be 1
		assertEquals(new Long(1), cassandraManager.getCounterValue(1234L, "test_column_1"));
		//Increment again
		cassandraManager.incrementCounter(1234L, "test_column_1");
		//Now the value should be 2
		assertEquals(new Long(2), cassandraManager.getCounterValue(1234L, "test_column_1"));
	}

	@Test
	public void testDecrementCounter() {
		String columnFamily = "testDecrementCounter";

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);
		
		//Created a new counter with the starting value 10
		cassandraManager.updateCounter(1111L, "test_column_0", 10);
		//Value must be 10
		assertEquals(new Long(10), cassandraManager.getCounterValue(1111L, "test_column_0"));
		//Increment counter
		cassandraManager.decrementCounter(1111L, "test_column_0");
		//Value must be 11
		assertEquals(new Long(9), cassandraManager.getCounterValue(1111L, "test_column_0"));
		
		//Decrement a counter that does not exist
		cassandraManager.decrementCounter(1234L, "test_column_1");
		//Reading a counter that does not exist
		assertEquals(new Long(-1), cassandraManager.getCounterValue(1234L, "test_column_1"));
		//Decrement again
		cassandraManager.decrementCounter(1234L, "test_column_1");
		//Now the value should be -2
		assertEquals(new Long(-2), cassandraManager.getCounterValue(1234L, "test_column_1"));
	}
	
	
	/**
	 * None of these test should have a secondary index test
	 */
	protected List<String> getSecondaryIndexExcludeList() {
		ArrayList<String> excludeList = new ArrayList<String>();
		excludeList.add("testCounterUpdate");
		excludeList.add("testDeleteCounterColumn");
		excludeList.add("testReadNonExistentCounterColumn");
		excludeList.add("testIncrementCounter");
		excludeList.add("testDecrementCounter");
		return excludeList;
	}

	/**
	 * All these should have "CounterColumnType" column value type.
	 */
	protected Map<String, String> getColumnValueTypeOverrides() {
		HashMap<String, String> valueTypes = new HashMap<String, String>();
		valueTypes.put("testCounterUpdate", "CounterColumnType");
		valueTypes.put("testDeleteCounterColumn", "CounterColumnType");
		valueTypes.put("testReadNonExistentCounterColumn", "CounterColumnType");
		valueTypes.put("testIncrementCounter", "CounterColumnType");
		valueTypes.put("testDecrementCounter", "CounterColumnType");
		return valueTypes;
	}

	protected Map<String, Map<String, Object>> getData(String columnFamilyName) {
		return null;
	}

	protected void tearDown() {
		// Have provide implementation of the abstract method in
		// CassandraTestBase
		// Nothing to do!
	}

}
