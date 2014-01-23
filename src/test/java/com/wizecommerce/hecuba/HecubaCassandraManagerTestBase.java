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

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang.time.DateUtils;
import com.wizecommerce.hecuba.util.CassandraTestBase;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static junit.framework.Assert.*;

public abstract class HecubaCassandraManagerTestBase extends CassandraTestBase {

	// Apparently, cassandra can not handle larger integer set as TTL. So we are limiting the TTL to 10 years (which
	// technically is equal to not setting TTL at all :).
	final int TEN_YEARS = 3600 * 24 * 365 * 10;

	protected HecubaCassandraManagerTestBase(String className) throws IOException {
		super(className);
	}


	public void testConstructor() {

		String clusterName = "My Awesome Cluster";
		String locationURL = "africa:northamerica:southamerica:asia:europe";
		String thriftPorts = "3726";
		String keyspace = "WizeCommerce";
		String cf = "Platform and Infra";
		String secondaryIndexColumns = "Column_1:Column_2:Column_3";

		CassandraParamsBean bean = new CassandraParamsBean();
		bean.setClustername(clusterName);
		bean.setLocationURLs(locationURL);
		bean.setThriftPorts(thriftPorts);
		bean.setKeyspace(keyspace);
		bean.setCf(cf);
		bean.setSiColumns(secondaryIndexColumns);

		assertEquals(clusterName, bean.getClustername());
		assertEquals(locationURL, bean.getLocationURLs());
		assertEquals(thriftPorts, bean.getThriftPorts());
		assertEquals(keyspace, bean.getKeyspace());
		assertEquals(cf, bean.getCf());
		assertEquals(secondaryIndexColumns, bean.getSiColumns());


		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(bean);
		assertEquals(clusterName, cassandraManager.getClusterName());
		assertEquals(locationURL, cassandraManager.getLocationURL());
		assertEquals(thriftPorts, cassandraManager.getPort());
		assertEquals(keyspace, cassandraManager.getKeyspace());
		assertEquals(cf, cassandraManager.getColumnFamilyName());
		assertTrue(ListUtils.removeAll(Arrays.asList(secondaryIndexColumns.split(":")),
									   cassandraManager.getColumnsToIndexOnColumnNameAndValue()).size() == 0);

	}


	/**
	 * Runs the secondary index tests with columnSize overrides enabled.
	 */
	/**
	 * testSecondaryIndexWithMaxColumnsOverride
	 */
	@Test
	public void testUpdateRowScenario16() {
		String columnFamily = "testUpdateRowScenario16";

		final int maxResultSetSize = 120;

		CassandraParamsBean bean = new CassandraParamsBean();
		bean.setClustername(CLUSTER_NAME);
		bean.setLocationURLs(LOCATION);
		bean.setThriftPorts(PORT);
		bean.setKeyspace(KEYSPACE);
		bean.setMaxSiColumnCount(maxResultSetSize);
		bean.setMaxColumnCount(maxResultSetSize);
		bean.setCf(columnFamily);
		bean.setSiColumns("MySecondaryKey_1:MySecondaryKey_2");

		//Run all generic tests to verify standard behavior is still functional.
		runSecondaryIndexTests(bean);

		//Large SecondIndex result set.
		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(bean);

		//Write 120 rows to cassandra, with the same secondary Index.
		for (long i = 0; i < maxResultSetSize; i++) {
			HashMap<String, Object> row = new HashMap<String, Object>();
			row.put("column_1", "value_1");
			row.put("column_2", "value_2");
			row.put("MySecondaryKey_1", "MySecondaryKey_1_value_1");
			cassandraManager.updateRow(i, row);
		}

		// try to retrieve data by secondaryIndex value.
		CassandraResultSet<Long, String> results = cassandraManager.retrieveBySecondaryIndex("MySecondaryKey_1",
																							 "MySecondaryKey_1_value_1");
		int count = 1;
		while (results.hasNextResult()) {
			results.nextResult();
			count++;
		}
		assertEquals(count, maxResultSetSize);

		//Write 120 Columns to a single row
		HashMap<String, Object> row = new HashMap<String, Object>();
		final long now = System.currentTimeMillis();
		for (int i = 0; i < maxResultSetSize; i++) {
			row.put(String.valueOf(i), String.valueOf(i) + "_" + now);
		}
		cassandraManager.deleteRow(1234L); //clearing old data
		cassandraManager.updateRow(1234L, row);

		//Attempt to retrieve all 120 columns that were written.
		Random random = new Random(System.currentTimeMillis());
		try {
			results = cassandraManager.readAllColumns(1234L);
			assertEquals(results.getColumnNames().size(), maxResultSetSize);
			//read a random value and verify the expected value.
			int index = random.nextInt(maxResultSetSize);
			assertEquals(results.getString(String.valueOf(index)), String.valueOf(index) + "_" + now);
		} catch (Exception e) {
			e.printStackTrace();
		}


	}

	@Test
	public void testSecondaryIndexWithDeletes() {
		String columnFamily = "testSecondaryIndexWithDeletes";

		final int maxResultSetSize = 120;

		CassandraParamsBean bean = new CassandraParamsBean();
		bean.setClustername(CLUSTER_NAME);
		bean.setLocationURLs(LOCATION);
		bean.setThriftPorts(PORT);
		bean.setKeyspace(KEYSPACE);
		bean.setMaxSiColumnCount(maxResultSetSize);
		bean.setMaxColumnCount(maxResultSetSize);
		bean.setCf(columnFamily);
		bean.setSiColumns("MySecondaryKey_1:MySecondaryKey_2");

		// retrieve the cassandra manager.
		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(bean);

		/**
		 * 1. Test whether delete row works.
		 */

		// insert the record 1234L with this secondary index.
		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("column_1", "value_1");
		row.put("column_2", "value_2");
		row.put("MySecondaryKey_1", "MySecondaryKey_1_value_1");
		cassandraManager.updateRow(1234L, row);

		// now lets see whether we can retrieve this item by the secondary index.
		final CassandraResultSet<Long, String> objectRetrievedBeforeDeletion =
				cassandraManager.retrieveBySecondaryIndex("MySecondaryKey_1", "MySecondaryKey_1_value_1");

		//   1. we should have retrieved exactly one
		assertEquals(false, objectRetrievedBeforeDeletion.hasNextResult());

		//   2. And that should have the id 1234L
		assertEquals(new Long(1234), objectRetrievedBeforeDeletion.getKey());

		//   3. And that should have other columns too.
		assertEquals("value_1", objectRetrievedBeforeDeletion.getString("column_1"));
		assertEquals("value_2", objectRetrievedBeforeDeletion.getString("column_2"));
		assertEquals("MySecondaryKey_1_value_1", objectRetrievedBeforeDeletion.getString("MySecondaryKey_1"));

		// now lets delete this item.
		cassandraManager.deleteRow(1234L);

		// can we retrieve it now?
		final CassandraResultSet<Long, String> objectRetrievedAfterDeletion = cassandraManager.retrieveBySecondaryIndex(
				"MySecondaryKey_1", "MySecondaryKey_1_value_1");

		assertEquals(null, objectRetrievedAfterDeletion);

		/**
		 * 1. Test whether delete column works.
		 */

		// now lets add a new record.
		row = new HashMap<String, Object>();
		row.put("column_1", "value_1");
		row.put("column_2", "value_2");
		row.put("MySecondaryKey_2", "MySecondaryKey_2_value_2");
		cassandraManager.updateRow(1978L, row);

		// now lets see whether we can retrieve this item by the secondary index.
		final CassandraResultSet<Long, String> objectRetrievedBeforeColumnDeletion =
				cassandraManager.retrieveBySecondaryIndex("MySecondaryKey_2", "MySecondaryKey_2_value_2");

		//   1. we should have retrieved exactly one
		assertEquals(false, objectRetrievedBeforeColumnDeletion.hasNextResult());

		//   2. And that should have the id 1234L
		assertEquals(new Long(1978L), objectRetrievedBeforeColumnDeletion.getKey());

		// alright, now lets delete this secondary index column.
		cassandraManager.deleteColumn(1978L, "MySecondaryKey_2");

		// now lets see whether we can retrieve 1978L with this secondary index query.
		final CassandraResultSet<Long, String> objectRetrievedAfterColumnDeletion =
				cassandraManager.retrieveBySecondaryIndex("MySecondaryKey_2", "MySecondaryKey_2_value_2");

		assertEquals(null, objectRetrievedAfterColumnDeletion);


	}

	/**
	 * testSecondaryIndexWithUpdatesToMultipleColumns
	 */
	@Test
	public void testUpdateRowScenario15() {
		String columnFamily = "testUpdateRowScenario15";

		CassandraParamsBean bean = new CassandraParamsBean();
		bean.setClustername(CLUSTER_NAME);
		bean.setLocationURLs(LOCATION);
		bean.setThriftPorts(PORT);
		bean.setKeyspace(KEYSPACE);
		bean.setCf(columnFamily);
		bean.setSiColumns("MySecondaryKey_1:MySecondaryKey_2");
		runSecondaryIndexTests(bean);


	}

	public void runSecondaryIndexTests(CassandraParamsBean bean) {


		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(bean);


		HashMap<String, Object> row = new HashMap<String, Object>();
		row.put("column_1", "value_1");
		row.put("column_2", "value_2");
		row.put("MySecondaryKey_1", "MySecondaryKey_1_value_1");
		cassandraManager.updateRow(1234L, row);

		/**
		 *  _______________________________________________
		 *  |   MySecondaryKey_1:MySecondaryKey_1_value_1 | --> 1234
		 *  -----------------------------------------------
		 */

		// try to retrieve that by its id.
		CassandraResultSet<Long, String> resultFromFirstRetrieval = cassandraManager.retrieveBySecondaryIndex(
				"MySecondaryKey_1", "MySecondaryKey_1_value_1");
		//   1. we should have retrieved exactly one
		assertEquals(false, resultFromFirstRetrieval.hasNextResult());

		//   2. And that should have the id 1234L
		assertEquals(new Long(1234), resultFromFirstRetrieval.getKey());

		//   3. And that should have other columns too.
		assertEquals("value_1", resultFromFirstRetrieval.getString("column_1"));
		assertEquals("value_2", resultFromFirstRetrieval.getString("column_2"));
		assertEquals("MySecondaryKey_1_value_1", resultFromFirstRetrieval.getString("MySecondaryKey_1"));

		// add another row with the same value for the secondary index
		row = new HashMap<String, Object>();
		row.put("column_3", "value_3");
		row.put("column_4", "value_4");
		row.put("MySecondaryKey_1", "MySecondaryKey_1_value_1");
		cassandraManager.updateRow(1233L, row);

		/**
		 *  _______________________________________________
		 *  |   MySecondaryKey_1:MySecondaryKey_1_value_1 | --> 1234, 1233
		 *  -----------------------------------------------
		 */

		//BEGIN CHANGES
		// see whether we can retrieve both
		CassandraResultSet<Long, String> resultFromSecondRetrieval = cassandraManager.retrieveBySecondaryIndex(
				"MySecondaryKey_1", "MySecondaryKey_1_value_1");

		//   1. And that should have the id 1234L and 1233L
		if (resultFromSecondRetrieval.getKey() == 1234L) {
			logger.info("Testing 1234");
			assertEquals("value_1", resultFromSecondRetrieval.getString("column_1"));
			assertEquals("value_2", resultFromSecondRetrieval.getString("column_2"));
			assertEquals("MySecondaryKey_1_value_1", resultFromSecondRetrieval.getString("MySecondaryKey_1"));
		} else {
			logger.info("Testing 1233");
			assertEquals("value_3", resultFromSecondRetrieval.getString("column_3"));
			assertEquals("value_4", resultFromSecondRetrieval.getString("column_4"));
			assertEquals("MySecondaryKey_1_value_1", resultFromSecondRetrieval.getString("MySecondaryKey_1"));
		}

		//   2. we should have retrieved 2 results.
		assertTrue(resultFromSecondRetrieval.hasNextResult());
		resultFromSecondRetrieval.nextResult();

		if (resultFromSecondRetrieval.getKey() == 1234L) {
			logger.info("Testing 1234");
			assertEquals("value_1", resultFromSecondRetrieval.getString("column_1"));
			assertEquals("value_2", resultFromSecondRetrieval.getString("column_2"));
			assertEquals("MySecondaryKey_1_value_1", resultFromSecondRetrieval.getString("MySecondaryKey_1"));
		} else {
			logger.info("Testing 1233");
			assertEquals("value_3", resultFromSecondRetrieval.getString("column_3"));
			assertEquals("value_4", resultFromSecondRetrieval.getString("column_4"));
			assertEquals("MySecondaryKey_1_value_1", resultFromSecondRetrieval.getString("MySecondaryKey_1"));
		}

		// change the value of the secondary index in one of the rows
		row = new HashMap<String, Object>();
		row.put("column_4", "value_44");
		row.put("MySecondaryKey_1", "MySecondaryKey_1_value_2");
		cassandraManager.updateRow(1233L, row);

		/**
		 *  __________________________________________
		 *  |   MySecondaryKey_1:MySecondaryKey_1_value_1 | --> 1234
		 *  ------------------------------------------
		 *
		 *  ____________________________________________
		 *  |   MySecondaryKey_1:MySecondaryKey_1_value_2 | --> 1233
		 *  --------------------------------------------
		 */


		// retrieve the row from the new secondary value
		CassandraResultSet<Long, String> resultFromThirdRetrieval = cassandraManager.retrieveBySecondaryIndex(
				"MySecondaryKey_1", "MySecondaryKey_1_value_2");

		//   2. And that should have the id 1233L
		assertEquals(new Long(1233), resultFromThirdRetrieval.getKey());

		//   3. And that should have other columns too.
		assertEquals("value_3", resultFromThirdRetrieval.getString("column_3"));
		assertEquals("value_44", resultFromThirdRetrieval.getString("column_4"));
		assertEquals("MySecondaryKey_1_value_2", resultFromThirdRetrieval.getString("MySecondaryKey_1"));


		// try to retrieve from the previous one and see whether it returns only one.
		CassandraResultSet<Long, String> resultFromFourthRetrieval = cassandraManager.retrieveBySecondaryIndex(
				"MySecondaryKey_1", "MySecondaryKey_1_value_1");

		//   1. we should have retrieved exactly one
		assertEquals(false, resultFromFourthRetrieval.hasNextResult());

		//   2. And that should have the id 1234L and 1233L
		assertEquals(new Long(1234), resultFromFourthRetrieval.getKey());

		// change the value of the secondary index for the first row above and see whether the initial query returns
		// anything now.
		cassandraManager.updateString(1234L, "MySecondaryKey_1", "MySecondaryKey_1_value_3");

		/**
		 *  __________________________________________
		 *  |   MySecondaryKey_1:MySecondaryKey_1_value_1 | -->
		 *  ------------------------------------------
		 *
		 *  ____________________________________________
		 *  |   MySecondaryKey_1:MySecondaryKey_1_value_2 | --> 1233
		 *  --------------------------------------------
		 *
		 *  ____________________________________________
		 *  |   MySecondaryKey_1:MySecondaryKey_1_value_3 | --> 1234
		 *  --------------------------------------------
		 *
		 */

		CassandraResultSet<Long, String> resultFromFifthRetrieval = cassandraManager.retrieveBySecondaryIndex(
				"MySecondaryKey_1", "MySecondaryKey_1_value_1");

		//   1. we should have retrieved none
		assertEquals(null, resultFromFifthRetrieval);

		resultFromFifthRetrieval = cassandraManager.retrieveBySecondaryIndex("MySecondaryKey_1",
																			 "MySecondaryKey_1_value_2");

		//   1. we should have retrieved one
		assertEquals(false, resultFromFifthRetrieval.hasNextResult());

		resultFromFifthRetrieval = cassandraManager.retrieveBySecondaryIndex("MySecondaryKey_1",
																			 "MySecondaryKey_1_value_3");

		//   1. we should have retrieved one
		assertEquals(false, resultFromFifthRetrieval.hasNextResult());

	}

	/**
	 * ********************************************************************************************************
	 *
	 * Column name based secondary index tests
	 *
	 * ********************************************************************************************************
	 */

	/**
	 * testColumnNameBasedSecondaryIndexBasics
	 */
	@Test
	public void testUpdateRowScenario14() {
		String columnFamily = "testUpdateRowScenario14";

		CassandraParamsBean bean = new CassandraParamsBean();
		bean.setClustername(CLUSTER_NAME);
		bean.setLocationURLs(LOCATION);
		bean.setThriftPorts(PORT);
		bean.setKeyspace(KEYSPACE);
		bean.setCf(columnFamily);

		// create secondary index on all column names.
		bean.setSiByColumnsPattern(".*");
		bean.setSiColumns("MySecondaryKey_1:MySecondaryKey_2");
		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(bean);

		HashMap<String, Object> row = new HashMap<String, Object>();

		List<Long> listOfIds = Lists.newArrayList(123L, 124L, 126L, 127L, 3456L, 4567L);

		for (Long id : listOfIds) {
			row.clear();
			row.put("column_1", "value_1");

			if (id < 130) {
				row.put("column_2", "value_2");
			}
			cassandraManager.updateRow(id, row);
		}

		// ***************************************************************************************
		// Scenario 1: Check with the first column name.
		// ***************************************************************************************

		// first retrieve by column 1
		CassandraResultSet<Long, String> idsWithColumn1 = cassandraManager.retrieveByColumnNameBasedSecondaryIndex(
				"column_1");


		// has it got some results.
		assertNotNull(idsWithColumn1);
		assertTrue(idsWithColumn1.hasResults());

		int count = 0;
		while (idsWithColumn1.hasResults()) {
			Long key = idsWithColumn1.getKey();

			count++;

			// make sure this id is part of the original list
			assertTrue(listOfIds.indexOf(key) > -1);
			if (idsWithColumn1.hasNextResult()) {
				idsWithColumn1.nextResult();
			} else {
				break;
			}
		}

		// make sure we retrieved all of the ids.
		assertEquals(count, listOfIds.size());

		// ***************************************************************************************
		// Scenario 2: Check with the second column name.
		// ***************************************************************************************

		// retrieve by column 2
		CassandraResultSet<Long, String> idsWithColumn2 = cassandraManager.retrieveByColumnNameBasedSecondaryIndex(
				"column_2");


		// has it got some results.
		assertTrue(idsWithColumn2.hasResults());

		count = 0;
		while (idsWithColumn2.hasResults()) {
			Long key = idsWithColumn2.getKey();

			count++;

			// make sure this id is part of the original list
			assertTrue(listOfIds.indexOf(key) > -1);
			assertTrue(key < 130);
			if (idsWithColumn2.hasNextResult()) {
				idsWithColumn2.nextResult();
			} else {
				break;
			}
		}

		assertEquals(4, count);

		// ***************************************************************************************
		// Scenario 3: Delete the column 1 from 3456L and 123L
		// ***************************************************************************************

		cassandraManager.deleteColumn(123L, "column_1");
		cassandraManager.deleteColumn(3456L, "column_1");

		// retrieve by column 1
		idsWithColumn1 = cassandraManager.retrieveByColumnNameBasedSecondaryIndex("column_1");


		// has it got some results.
		assertTrue(idsWithColumn1.hasResults());

		count = 0;
		while (idsWithColumn1.hasResults()) {
			Long key = idsWithColumn1.getKey();

			count++;

			// make sure this id is part of the original list
			assertTrue(listOfIds.indexOf(key) > -1);
			assertTrue(key != 123L && key != 3456L);
			if (idsWithColumn1.hasNextResult()) {
				idsWithColumn1.nextResult();
			} else {
				break;
			}
		}

		assertEquals(4, count);

		// ***************************************************************************************
		// Scenario 3: Retrieve by a secondary index that doesn't exist
		// ***************************************************************************************
		idsWithColumn1 = cassandraManager.retrieveByColumnNameBasedSecondaryIndex("column_33");

		// it shouldn't have any results.
		assertNull(idsWithColumn1);

	}


	@Test
	public void testReadColumnSlice() {
		String columnFamily = "testReadColumnSlice";
		logger.info("Testing Hector read column slice");

		CassandraParamsBean bean = new CassandraParamsBean();
		bean.setClustername(CLUSTER_NAME);
		bean.setLocationURLs(LOCATION);
		bean.setThriftPorts(PORT);
		bean.setKeyspace(KEYSPACE);
		bean.setCf(columnFamily);

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(bean);

		HashMap<String, Object> row = new HashMap<String, Object>();

		/*********************************************************/
		/****** TEST CASSANDRA ROW CONTAINS > 100 COLUMNS ********/
		/*********************************************************/
		// Insert 200 columns in cassandra in a single row
		int noOfColumns = 200;
		Long key = 1234L;
		for (int i = 1; i <= noOfColumns; i++) {
			row.put("column_" + i, "value_" + i);
		}
		cassandraManager.updateRow(key, row);

		/**
		 *  ____________________________________________________________
		 *  | Retrieve all columns for the key (no of columns < 10000)
		 *  ------------------------------------------------------------
		 */
		// Retrieve all columns
		CassandraResultSet<Long, String> result = cassandraManager.readColumnSliceAllColumns(key);
		// Column count should be 200
		assertEquals(noOfColumns, result.getColumnNames().size());
		for (String columnName : row.keySet()) {
			assertEquals(row.get(columnName), result.getString(columnName));
		}

		/**
		 *  __________________________________
		 *  | Retrieve N columns for the key
		 *  ----------------------------------
		 */
		// Retrieve 120 columns
		result = cassandraManager.readColumnSlice(key, null, null, false, 120);
		// Column count should be 120
		assertEquals(120, result.getColumnNames().size());

		/**
		 *  _________________________________________________
		 *  | Retrieve N columns with column range specified
		 *  -------------------------------------------------
		 */
		// Columns in a range with limit as 10
		result = cassandraManager.readColumnSlice(key, "column_111", "column_113", false, 10);
		// This range contains 3 columns, so the column count should be 3
		assertEquals(3, result.getColumnNames().size());
		assertEquals("value_111", result.getString("column_111"));
		assertEquals("value_112", result.getString("column_112"));
		assertEquals("value_113", result.getString("column_113"));


		/*********************************************************/
		/****** TEST CASSANDRA ROW CONTAINS < 100 COLUMNS ********/
		/*********************************************************/
		/**
		 *  __________________________________________________
		 *  | Retrieve all columns for row with < 100 columns
		 *  --------------------------------------------------
		 */
		row.clear();
		key = 5678L;
		for (int i = 1; i <= 80; i++) {  // < 100 columns (80 in this example)
			row.put("column_" + i, "value_" + i);
		}
		cassandraManager.updateRow(key, row);
		result = cassandraManager.readColumnSliceAllColumns(key);
		// Column count should be 80
		assertEquals(80, result.getColumnNames().size());
		for (String columnName : row.keySet()) {
			assertEquals(row.get(columnName), result.getString(columnName));
		}


		/**
		 *  __________________________________
		 *  | Retrieve N columns for the key
		 *  ----------------------------------
		 */
		// Retrieve 50 columns
		result = cassandraManager.readColumnSlice(key, null, null, false, 50);
		// Column count should be 120
		assertEquals(50, result.getColumnNames().size());

		/**
		 *  _________________________________________________
		 *  | Retrieve N columns with column range specified
		 *  -------------------------------------------------
		 */
		// Columns in a range with limit as 10
		result = cassandraManager.readColumnSlice(key, "column_11", "column_13", false, 10);
		// This range contains 3 columns, so the column count should be 3
		assertEquals(3, result.getColumnNames().size());
		assertEquals("value_11", result.getString("column_11"));
		assertEquals("value_12", result.getString("column_12"));
		assertEquals("value_13", result.getString("column_13"));


		/*************************************************************/
		/****** TEST CASSANDRA ROW DOES NOT EXIST (CACHE MISS) *******/
		/*************************************************************/
		row.clear();
		key = 9999L;
		result = cassandraManager.readColumnSliceAllColumns(key);
		assertEquals(0, result.getColumnNames().size());

		result = cassandraManager.readColumnSlice(key, null, null, false, 50);
		assertEquals(0, result.getColumnNames().size());

		result = cassandraManager.readColumnSlice(key, "column_11", "column_13", false, 10);
		assertEquals(0, result.getColumnNames().size());

	}

	@Test
	public void testReadColumnSliceMultipleKeys() {
		String columnFamily = "testReadColumnSliceMultipleKeys";
		logger.info("Testing Hector read column slice with multiple keys");

		CassandraParamsBean bean = new CassandraParamsBean();
		bean.setClustername(CLUSTER_NAME);
		bean.setLocationURLs(LOCATION);
		bean.setThriftPorts(PORT);
		bean.setKeyspace(KEYSPACE);
		bean.setCf(columnFamily);

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(bean);
		HashMap<String, Object> row1 = new HashMap<String, Object>();

		/**********************************************************/
		/****** TEST CASSANDRA ROWS CONTAINS > 100 COLUMNS ********/
		/**********************************************************/

		Long key1 = 1234L;
		// Insert 200 columns in cassandra for key1
		for (int i = 1; i <= 200; i++) {
			row1.put("column_" + i, "value_" + i);
		}
		cassandraManager.updateRow(key1, row1);

		// Insert 150 columns in cassandra for key2
		HashMap<String, Object> row2 = new HashMap<String, Object>();
		Long key2 = 5678L;
		for (int i = 1; i <= 150; i++) {
			row2.put("column_" + i, "value_" + i);
		}
		cassandraManager.updateRow(key2, row2);

		Set<Long> keys = new HashSet<Long>();
		keys.add(key1);
		keys.add(key2);


		/**
		 *  ___________________________________________________________
		 *  | Retrieve all columns for all keys (no of columns < 10000)
		 *  -----------------------------------------------------------
		 */
		CassandraResultSet<Long, String> result = cassandraManager.readColumnSliceAllColumns(keys);
		Set<Long> keysCopy = new HashSet<Long>(keys);
		int count = 0;
		assertNotNull(result);

		/**
		 * NOTE: We are using [while(true) --> break] loop here because CassandraResultSet points to the first row
		 * when its created.
		 * CassandraResultSet.hasNextResult returns true if there are any more rows and CassandraResultSet.nextResult
		 * () moves the pointer
		 * to the next row. So in the loop, we needed the processing --> check break condition --> move the pointer.
		 */
		while (true) {
			count++;
			assertEquals(true, keysCopy.remove(result.getKey()));
			if (key1.equals(result.getKey())) {
				// Column count should be 200
				assertEquals(200, result.getColumnNames().size());
				for (String columnName : row1.keySet()) {
					assertEquals(row1.get(columnName), result.getString(columnName));
				}
			} else if (key2.equals(result.getKey())) {
				// Column count should be 150
				assertEquals(150, result.getColumnNames().size());
				for (String columnName : row2.keySet()) {
					assertEquals(row2.get(columnName), result.getString(columnName));
				}
			}
			if (!result.hasNextResult()) {
				break;
			}
			result.nextResult();
		}
		assertEquals(keys.size(), count); // Asserts the result set contains exactly same number of rows as is the
		// keys size
		assertEquals(0, keysCopy.size()); // Asserts we get both the keys in the result set


		/**
		 *  ___________________________________
		 *  |  Retrieve N columns for all keys
		 *  -----------------------------------
		 */
		result = cassandraManager.readColumnSlice(keys, null, null, false, 120);
		keysCopy = new HashSet<Long>(keys);
		count = 0;
		while (true) {
			count++;
			// column count should be 120 since we put the limit of 120
			assertEquals(120, result.getColumnNames().size());
			assertEquals(true, keysCopy.remove(result.getKey()));
			if (!result.hasNextResult()) {
				break;
			}
			result.nextResult();
		}
		assertEquals(keys.size(), count); // Asserts the result set contains exactly same number of rows as is the
		// keys size
		assertEquals(0, keysCopy.size()); // Asserts we get both the keys in the result set


		/**
		 *  ________________________________________________________
		 *  | Retrieve N columns with range specified for all keys
		 *  --------------------------------------------------------
		 */
		result = cassandraManager.readColumnSlice(keys, "column_111", "column_113", false, 10);
		keysCopy = new HashSet<Long>(keys);
		count = 0;
		while (true) {
			count++;
			// This range contains 3 columns, so the column count should be 3
			assertEquals(3, result.getColumnNames().size());
			assertEquals("value_111", result.getString("column_111"));
			assertEquals("value_112", result.getString("column_112"));
			assertEquals("value_113", result.getString("column_113"));
			assertEquals(true, keysCopy.remove(result.getKey()));
			if (!result.hasNextResult()) {
				break;
			}
			result.nextResult();
		}
		assertEquals(keys.size(), count); // Asserts the result set contains exactly same number of rows as is the
		// keys size
		assertEquals(0, keysCopy.size()); // Asserts we get both the keys in the result set


		/*************************************************************************/
		/****** TEST CASSANDRA ROWS CONTAINS < 100 COLUMNS  +  A MISS ROW ********/
		/*************************************************************************/
		cassandraManager.deleteRow(key1);
		cassandraManager.deleteRow(key2);
		long key3 = 9999L;
		keys.add(key3);

		row1.clear();
		for (int i = 1; i <= 80; i++) {
			row1.put("column_" + i, "value_" + i);
		}
		cassandraManager.updateRow(key1, row1);

		row2.clear();
		for (int i = 1; i <= 60; i++) {
			row2.put("column_" + i, "value_" + i);
		}
		cassandraManager.updateRow(key2, row2);

		/**
		 *  ____________________________________
		 *  | Retrieve all columns for all keys
		 *  ------------------------------------
		 */
		result = cassandraManager.readColumnSliceAllColumns(keys);
		keysCopy = new HashSet<Long>(keys);
		count = 0;
		assertNotNull(result);

		/**
		 * NOTE: We are using [while(true) --> break] loop here because CassandraResultSet points to the first row
		 * when its created.
		 * CassandraResultSet.hasNextResult returns true if there are any more rows and CassandraResultSet.nextResult
		 * () moves the pointer
		 * to the next row. So in the loop, we needed the processing --> check break condition --> move the pointer.
		 */
		while (true) {
			count++;
			assertEquals(true, keysCopy.remove(result.getKey()));
			if (result.getKey() == key1) {
				// Column count should be 80
				assertEquals(80, result.getColumnNames().size());
				for (String columnName : row1.keySet()) {
					assertEquals(row1.get(columnName), result.getString(columnName));
				}
			} else if (result.getKey() == key2) {
				// Column count should be 60
				assertEquals(60, result.getColumnNames().size());
				for (String columnName : row2.keySet()) {
					assertEquals(row2.get(columnName), result.getString(columnName));
				}
			} else if (result.getKey() == key3) {
				// Column count should be 0 since key3 doesn't exist
				assertEquals(0, result.getColumnNames().size());
			}
			if (!result.hasNextResult()) {
				break;
			}
			result.nextResult();
		}
		assertEquals(3, count); // Asserts the result set contains exactly 3 rows
		assertEquals(0, keysCopy.size()); // Asserts we get all the keys in the result set


		/**
		 *  ___________________________________
		 *  |  Retrieve N columns for all keys
		 *  -----------------------------------
		 */
		result = cassandraManager.readColumnSlice(keys, null, null, false, 50);
		keysCopy = new HashSet<Long>(keys);
		count = 0;
		while (true) {
			count++;
			assertEquals(true, keysCopy.remove(result.getKey()));
			if (result.getKey() == key1 || result.getKey() == key2) {
				// column count should be 50 (for key1 & key2) since we passed the limit 50
				assertEquals(50, result.getColumnNames().size());
			} else if (result.getKey() == key3) {
				// Column count should be 0 since this key doesn't exist
				assertEquals(0, result.getColumnNames().size());
			}
			if (!result.hasNextResult()) {
				break;
			}
			result.nextResult();
		}
		assertEquals(3, count); // Asserts the result set contains exactly 3 rows
		assertEquals(0, keysCopy.size()); // Asserts we get all the keys in the result set


		/**
		 *  ________________________________________________________
		 *  | Retrieve N columns with range specified for all keys
		 *  --------------------------------------------------------
		 */
		result = cassandraManager.readColumnSlice(keys, "column_11", "column_13", false, 10);
		keysCopy = new HashSet<Long>(keys);
		count = 0;
		while (true) {
			count++;
			assertEquals(true, keysCopy.remove(result.getKey()));
			if (result.getKey() == key1 || result.getKey() == key2) {
				// This range contains 3 columns, so the column count should be 3 in case of key1 & key2
				assertEquals(3, result.getColumnNames().size());
				assertEquals("value_11", result.getString("column_11"));
				assertEquals("value_12", result.getString("column_12"));
				assertEquals("value_13", result.getString("column_13"));
			} else if (result.getKey() == key3) {
				// Column count should be 0 since this key doesn't exist
				assertEquals(0, result.getColumnNames().size());
			}
			if (!result.hasNextResult()) {
				break;
			}
			result.nextResult();
		}
		assertEquals(3, count); // Asserts the result set contains exactly 3 rows
		assertEquals(0, keysCopy.size()); // Asserts we get all the keys in the result set

	}

	@Test
	public void testReadAllColumns() throws Exception {
		String columnFamily = "testReadAllColumns";
		logger.info("Testing Hector readAllColumns for single key");

		CassandraParamsBean bean = new CassandraParamsBean();
		bean.setClustername(CLUSTER_NAME);
		bean.setLocationURLs(LOCATION);
		bean.setThriftPorts(PORT);
		bean.setKeyspace(KEYSPACE);
		bean.setCf(columnFamily);

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(bean);
		Map<String, Object> row = new HashMap<String, Object>();
		/*********************************************************/
		/****** TEST CASSANDRA ROW CONTAINS > 100 COLUMNS ********/
		/*********************************************************/
		// Insert 200 columns in cassandra in a single row
		int noOfColumns = 200;
		Long key = 1234L;
		for (int i = 1; i <= noOfColumns; i++) {
			row.put("column_" + i, "value_" + i);
			if (i <= 100) {
			}
		}
		cassandraManager.updateRow(key, row);


		/**
		 *  ___________________________________
		 *  | Retrieve all columns for the key
		 *  -----------------------------------
		 */
		//set maxColumnCount to 200 since there 200 columns in test row
		bean.setMaxColumnCount(200);
		// re-create cassandra manager with maxColumnCount set
		cassandraManager = getHecubaClientManager(bean);
		CassandraResultSet<Long, String> result = cassandraManager.readAllColumns(key);
		assertEquals(200, result.getColumnNames().size());
		for (String columnName : row.keySet()) {
			assertEquals(row.get(columnName), result.getString(columnName));
		}
	}

	@Test
	public void testReadColumnsMultipleKeys() throws Exception {
		String columnFamily = "testReadColumnsMultipleKeys";
		logger.info("Testing readColumns with multiple keys");

		CassandraParamsBean bean = new CassandraParamsBean();
		bean.setClustername(CLUSTER_NAME);
		bean.setLocationURLs(LOCATION);
		bean.setThriftPorts(PORT);
		bean.setKeyspace(KEYSPACE);
		bean.setCf(columnFamily);

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(bean);

		/***********************************/
		/****** TEST CASSANDRA ROWS ********/
		/***********************************/

		Map<Long, Map<String, Object>> dataSet = new HashMap<Long, Map<String, Object>>();
		int noOfColumns = 50;
		Random randomGen = new Random();
		for (int k = 1; k <= 30; k++) {
			Long key = (long) randomGen.nextInt(99999999);
			Map<String, Object> rowData = new HashMap<String, Object>();
			for (int i = 1; i <= noOfColumns; i++) {
				rowData.put("column_" + i, "value_" + (i + key));
			}
			cassandraManager.updateRow(key, rowData);
			dataSet.put(key, rowData);
		}

		int noOfKeysToFetch = 20;
		int count = 0;
		Set<Long> keys = new HashSet<Long>();
		for (Long key : dataSet.keySet()) { //Prepare set of 20 keys to fetch
			keys.add(key);
			count++;
			if (count >= noOfKeysToFetch) {
				break;
			}
		}

		final List<String> emptyColumnNamesList = Collections.<String>emptyList();
		final Set<Long> emptyKeysSet = Collections.<Long>emptySet();

		/**
		 * CASE 1
		 *  _______________________________________________________________
		 *  | Retrieve non-empty set of columns for non-empty set of keys |
		 *  ---------------------------------------------------------------
		 */
		List<String> columnNames = new ArrayList<String>();
		for (int i = 11; i <= 35; i++) { //Prepare set of 25 columns to fetch
			columnNames.add("column_" + i);
		}
		CassandraResultSet<Long, String> result = cassandraManager.readColumns(keys, columnNames);
		assertCassandraResultSet(result, dataSet, keys, columnNames);

		/**
		 * CASE 2
		 *  ______________________________________________
		 *  | Empty set of keys and empty set of columns |
		 *  ----------------------------------------------
		 */
		result = cassandraManager.readColumns(emptyKeysSet, emptyColumnNamesList);
		assertEmptyCassandraResultSet(result);

		/**
		 * CASE 3
		 *  ______________________________________________
		 *  | Empty set of keys but valid set of columns |
		 *  ----------------------------------------------
		 */
		result = cassandraManager.readColumns(emptyKeysSet, columnNames);
		assertEmptyCassandraResultSet(result);

		/**
		 * CASE 4
		 *  ______________________________________________
		 *  | Valid set of keys but empty set of columns |
		 *  ----------------------------------------------
		 */
		List<String> allColumnNames = new ArrayList<String>();
		for (int i = 1; i <= noOfColumns; i++) {
			allColumnNames.add("column_" + i);
		}
		result = cassandraManager.readColumns(keys, emptyColumnNamesList);
		assertCassandraResultSet(result, dataSet, keys, allColumnNames); //should return all columns

		/**
		 * CASE 5
		 *  _____________________________________
		 *  | Valid set of keys and all columns |
		 *  -------------------------------------
		 */
		result = cassandraManager.readColumns(keys, allColumnNames);
		assertCassandraResultSet(result, dataSet, keys, allColumnNames); //should return all columns

		/**
		 * CASE 6
		 *  ____________________________
		 *  | All keys and all columns |
		 *  ----------------------------
		 */
		result = cassandraManager.readColumns(dataSet.keySet(), allColumnNames);
		assertCassandraResultSet(result, dataSet, dataSet.keySet(), allColumnNames); //should return all columns

		/**
		 * CASE 7
		 *  ____________________________
		 *  | 1 key and all columns    |
		 *  ----------------------------
		 */
		Set<Long> oneKey = new HashSet<Long>();
		oneKey.add(dataSet.keySet().iterator().next());
		result = cassandraManager.readColumns(oneKey, allColumnNames);
		assertCassandraResultSet(result, dataSet, oneKey, allColumnNames); //should return all columns

		/**
		 * CASE 8
		 *  __________________________________
		 *  | 1 key and valid set of columns |
		 *  ----------------------------------
		 */
		result = cassandraManager.readColumns(oneKey, columnNames);
		assertCassandraResultSet(result, dataSet, oneKey, columnNames);

		/**
		 * CASE 9
		 *  __________________________________
		 *  | 1 key and empty set of columns |
		 *  ----------------------------------
		 */
		result = cassandraManager.readColumns(oneKey, emptyColumnNamesList);
		assertCassandraResultSet(result, dataSet, oneKey, allColumnNames); //should return all columns

		/**
		 * CASE 10
		 *  _____________________________
		 *  | 1 key and null columnList |
		 *  -----------------------------
		 */
		result = cassandraManager.readColumns(oneKey, null);
		assertCassandraResultSet(result, dataSet, oneKey, allColumnNames); //should return all columns

		/**
		 * CASE 11
		 *  _________________________________________
		 *  | valid set of keys and null columnList |
		 *  -----------------------------------------
		 */
		result = cassandraManager.readColumns(keys, null);
		assertCassandraResultSet(result, dataSet, keys, allColumnNames); //should return all columns

	}

	private <K, N> void assertCassandraResultSet(CassandraResultSet<K, N> result, Map<K, Map<N, Object>> dataSet,
												 Set<K> fetchedKeys, List<N> fetchedColumns) {
		assertNotNull(result);
		assertTrue(result.hasResults());
		assertFalse(CollectionUtils.isEmpty(result.getColumnNames()));
		Set<K> keysCopy = new HashSet<K>(fetchedKeys);
		int count = 0;
		/**
		 * NOTE: We are using [while(true) --> break] loop here because CassandraResultSet points to the first row
		 * when its created.
		 * CassandraResultSet.hasNextResult returns true if there are any more rows and CassandraResultSet.nextResult()
		 * moves the pointer to the next row.
		 * So in the loop, we needed the processing --> check break condition --> move the pointer.
		 */
		while (true) {
			K key = result.getKey();
			count++;
			assertEquals(true, keysCopy.remove(key));
			assertEquals(fetchedColumns.size(), result.getColumnNames().size());
			for (N columnName : fetchedColumns) {
				Object expectedValue = dataSet.get(key).get(columnName);
				assertNotNull(expectedValue);
				assertEquals(expectedValue, result.getString(columnName));
			}
			if (!result.hasNextResult()) {
				break;
			}
			result.nextResult();
		}
		assertEquals(fetchedKeys.size(),
					 count); // Asserts the result set contains exactly same number of rows as is the fetchedKeys size
		assertEquals(0, keysCopy.size()); // Asserts we get all the keys in the result set
	}

	private <K, N> void assertEmptyCassandraResultSet(CassandraResultSet<K, N> result) {
		assertNotNull(result);
		assertFalse(result.hasResults());
		assertTrue(CollectionUtils.isEmpty(result.getColumnNames()));
	}

	/**
	 * testSecondaryIndexWithUpdatesToSingleColumn
	 */
	@Test
	public void testUpdateRowScenario13() {
		String columnFamily = "testUpdateRowScenario13";

		CassandraParamsBean bean = new CassandraParamsBean();
		bean.setClustername(CLUSTER_NAME);
		bean.setLocationURLs(LOCATION);
		bean.setThriftPorts(PORT);
		bean.setKeyspace(KEYSPACE);
		bean.setCf(columnFamily);
		bean.setSiColumns("MySecondaryKey_1:MySecondaryKey_2");


		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(bean);

		cassandraManager.updateString(1234L, "test_column", "test_value");

		// add a secondary index column, for the first time.
		cassandraManager.updateString(1234L, "MySecondaryKey_1", "SecondaryIndexValue");

		/**
		 *  __________________________________________
		 *  |   MySecondaryKey_1:SecondaryIndexValue | --> 1234
		 *  ------------------------------------------
		 */

		// try to retrieve that by its id.
		CassandraResultSet<Long, String> resultFromFirstRetrieval = cassandraManager.retrieveBySecondaryIndex(
				"MySecondaryKey_1", "SecondaryIndexValue");

		//   result should have the id 1234L
		assertNotNull(resultFromFirstRetrieval);
		assertEquals(new Long(1234), resultFromFirstRetrieval.getKey());

		//   And that should have other columns too.
		assertEquals("test_value", resultFromFirstRetrieval.getString("test_column"));

		// add another row with the same value for the secondary index
		cassandraManager.updateString(1233L, "MySecondaryKey_1", "SecondaryIndexValue");
		cassandraManager.updateString(1233L, "test_column", "test_value_2");

		/**
		 *  __________________________________________
		 *  |   MySecondaryKey_1:SecondaryIndexValue | --> 1234, 1233
		 *  ------------------------------------------
		 */

		//   2. And that should have the id 1234L and 1233L
		resultFromFirstRetrieval = cassandraManager.retrieveBySecondaryIndex("MySecondaryKey_1", "SecondaryIndexValue");
		List<String> items = new ArrayList<String>();

		assertTrue(resultFromFirstRetrieval != null);
		//get first result.
		items.add(resultFromFirstRetrieval.getString("test_column"));

		while (resultFromFirstRetrieval.hasNextResult()) {
			resultFromFirstRetrieval.nextResult();
			items.add(resultFromFirstRetrieval.getString("test_column"));
		}

		assertEquals(items.size(), 2);
		//Since we can't really check each items, since order is not guaranteed.
		Collections.sort(items);
		assertTrue(Collections.binarySearch(items, "test_value_2") >= 0);
		assertTrue(Collections.binarySearch(items, "test_value") >= 0);
		//   3. And that should have other columns too.

		cassandraManager.updateString(1233L, "MySecondaryKey_1", "SecondaryIndexValue_2");
		/**
		 *  __________________________________________
		 *  |   MySecondaryKey_1:SecondaryIndexValue | --> 1234
		 *  ------------------------------------------
		 *
		 *  ____________________________________________
		 *  |   MySecondaryKey_1:SecondaryIndexValue_2 | --> 1233
		 *  --------------------------------------------
		 */


		// retrieve the row from the new secondary value
		CassandraResultSet<Long, String> resultFromThirdRetrieval = cassandraManager.retrieveBySecondaryIndex(
				"MySecondaryKey_1", "SecondaryIndexValue_2");

		//   1. we should have retrieved only one.
		assertEquals(false, resultFromThirdRetrieval.hasNextResult());

		//   2. And that should have the id 1233L
		assertEquals(new Long(1233), resultFromThirdRetrieval.getKey());

		//   3. And that should have other columns too.
		assertEquals("test_value_2", resultFromThirdRetrieval.getString("test_column"));


		// try to retrieve from the previous one and see whether it returns only one.
		CassandraResultSet<Long, String> resultFromFourthRetrieval = cassandraManager.retrieveBySecondaryIndex(
				"MySecondaryKey_1", "SecondaryIndexValue");

		//   2. And that should have the id 1234L and 1233L
		assertEquals(new Long(1234), resultFromFourthRetrieval.getKey());

		// change the value of the secondary index for the first row above and see whether the initial query returns
		// anything now.
		cassandraManager.updateString(1234L, "MySecondaryKey_1", "SecondaryIndexValue_3");

		/**
		 *  __________________________________________
		 *  |   MySecondaryKey_1:SecondaryIndexValue | -->
		 *  ------------------------------------------
		 *
		 *  ____________________________________________
		 *  |   MySecondaryKey_1:SecondaryIndexValue_2 | --> 1233
		 *  --------------------------------------------
		 *
		 *  ____________________________________________
		 *  |   MySecondaryKey_1:SecondaryIndexValue_3 | --> 1234
		 *  --------------------------------------------
		 *
		 */

		CassandraResultSet<Long, String> resultFromFifthRetrieval = cassandraManager.retrieveBySecondaryIndex(
				"MySecondaryKey_1", "SecondaryIndexValue");

		assertEquals(null, resultFromFifthRetrieval);

		resultFromFifthRetrieval = cassandraManager.retrieveBySecondaryIndex("MySecondaryKey_1",
																			 "SecondaryIndexValue_2");

		//   1. we should have retrieved one
		assertEquals(false, resultFromFifthRetrieval.hasNextResult());

		resultFromFifthRetrieval = cassandraManager.retrieveBySecondaryIndex("MySecondaryKey_1",
																			 "SecondaryIndexValue_3");

		//   1. we should have retrieved one
		assertEquals(false, resultFromFifthRetrieval.hasNextResult());
	}


	@Test
	public void testHecubaClientManager() {

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager("testHecubaClientManager");

		assertEquals(CLUSTER_NAME, cassandraManager.getClusterName());
		assertEquals(LOCATION, cassandraManager.getLocationURL());
		assertEquals(PORT, cassandraManager.getPort());
		assertEquals(KEYSPACE, cassandraManager.getKeyspace());

	}

	@Test
	public void testUpdateString() {
		String columnFamily = "testUpdateString";
		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);

		cassandraManager.updateString(1234L, "test_column", "test_value");
		assertEquals("test_value", cassandraManager.readString(1234L, "test_column"));
	}

	@Test
	public void testUpdateBoolean() {
		String columnFamily = "testUpdateBoolean";

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);

		cassandraManager.updateBoolean(1234L, "test_column_1", false);
		assertEquals(Boolean.FALSE, cassandraManager.readBoolean(1234L, "test_column_1"));
		cassandraManager.updateBoolean(1234L, "test_column_2", true);
		assertEquals(Boolean.TRUE, cassandraManager.readBoolean(1234L, "test_column_2"));
	}

	@Test
	public void testUpdateDate() {
		String columnFamily = "testUpdateDate";

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);

		Date date = new Date();
		cassandraManager.updateDate(1234L, "test_column_1", date);
		Date actualDate = cassandraManager.readDate(1234L, "test_column_1");
		assertEquals(HecubaConstants.DATE_FORMATTER.print(date.getTime()),
					 HecubaConstants.DATE_FORMATTER.print(actualDate.getTime()));
	}

	@Test
	public void testUpdateDouble() {
		String columnFamily = "testUpdateDouble";

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);

		cassandraManager.updateDouble(1234L, "test_column_1", 0.0);
		assertTrue(0.0 == cassandraManager.readDouble(1234L, "test_column_1"));
		cassandraManager.updateDouble(1234L, "test_column_2", 1245.1245);
		assertTrue(1245.1245 == cassandraManager.readDouble(1234L, "test_column_2"));
		cassandraManager.updateDouble(1234L, "test_column_3", -1.55);
		assertTrue(-1.55 == cassandraManager.readDouble(1234L, "test_column_3"));
	}

	@Test
	public void testUpdateLong() {
		String columnFamily = "testUpdateLong";

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);

		cassandraManager.updateLong(1234L, "test_column_1", 0L);
		assertTrue(0L == cassandraManager.readLong(1234L, "test_column_1"));
		cassandraManager.updateLong(1234L, "test_column_2", 123456789123456789L);
		assertTrue(123456789123456789L == cassandraManager.readLong(1234L, "test_column_2"));
		cassandraManager.updateLong(1234L, "test_column_3", -112233445566778899L);
		assertTrue(-112233445566778899L == cassandraManager.readLong(1234L, "test_column_3"));

	}

	@Test
	public void testUpdateInteger() {
		String columnFamily = "testUpdateInteger";

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);

		cassandraManager.updateInteger(1234L, "test_column_1", 0);
		assertTrue(0.0 == cassandraManager.readDouble(1234L, "test_column_1"));
		cassandraManager.updateInteger(1234L, "test_column_2", 1245);
		assertTrue(1245 == cassandraManager.readDouble(1234L, "test_column_2"));
		cassandraManager.updateInteger(1234L, "test_column_3", -1456);
		assertTrue(-1456 == cassandraManager.readDouble(1234L, "test_column_3"));
	}

	@Test
	public void testUpdateRow() {
		String columnFamily = "testUpdateRow";

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);

		// First lets add new columns and check it.
		Date currentDate = new Date();
		Map<String, Object> columnValues = new HashMap<String, Object>();
		columnValues.put("test_column_1", new Integer(0));
		columnValues.put("test_column_2", new Integer(6));
		columnValues.put("test_column_3", "Nextag");
		columnValues.put("test_column_4", 12112.121);
		columnValues.put("test_column_5", currentDate);
		cassandraManager.updateRow(12312L, columnValues);

		assertEquals(new Integer(0), cassandraManager.readInteger(12312L, "test_column_1"));
		assertEquals(new Integer(6), cassandraManager.readInteger(12312L, "test_column_2"));
		assertEquals("Nextag", cassandraManager.readString(12312L, "test_column_3"));
		assertEquals(new Double(12112.121), cassandraManager.readDouble(12312L, "test_column_4"));
		assertEquals(HecubaConstants.DATE_FORMATTER.print(currentDate.getTime()),
					 HecubaConstants.DATE_FORMATTER.print(cassandraManager.readDate(12312L, "test_column_5")
																				   .getTime()));

		// Now lets update certain columns and test it.
		columnValues = new HashMap<String, Object>();
		columnValues.put("test_column_1", new Integer(-1212));
		columnValues.put("test_column_3", "Nextag Goes Live");
		cassandraManager.updateRow(12312L, columnValues);

		assertEquals(new Integer(-1212), cassandraManager.readInteger(12312L, "test_column_1"));
		assertEquals("Nextag Goes Live", cassandraManager.readString(12312L, "test_column_3"));

	}

	@Test
	public void testDeleteColumn() {
		String columnFamily = "testDeleteColumn";

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);
		cassandraManager.updateString(1234L, "test_column", "test_value");
		assertEquals("test_value", cassandraManager.readString(1234L, "test_column"));

		cassandraManager.deleteColumn(1234L, "test_column");
		assertEquals(null, cassandraManager.readString(1234L, "test_column"));
	}

	@Test
	public void testDeleteRow() {
		String columnFamily = "testDeleteRow";

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);

		// First lets add new columns and check it.
		Date currentDate = new Date();
		Map<String, Object> columnValues = new HashMap<String, Object>();
		columnValues.put("test_column_1", new Integer(0));
		columnValues.put("test_column_2", new Integer(6));
		columnValues.put("test_column_3", "Nextag");
		columnValues.put("test_column_4", 12112.121);
		columnValues.put("test_column_5", currentDate);
		cassandraManager.updateRow(12312L, columnValues);

		// Just make sure we have the added row.
		assertEquals(new Integer(0), cassandraManager.readInteger(12312L, "test_column_1"));
		assertEquals(new Integer(6), cassandraManager.readInteger(12312L, "test_column_2"));
		assertEquals("Nextag", cassandraManager.readString(12312L, "test_column_3"));
		assertEquals(new Double(12112.121), cassandraManager.readDouble(12312L, "test_column_4"));
		assertEquals(HecubaConstants.DATE_FORMATTER.print(currentDate.getTime()),
					 HecubaConstants.DATE_FORMATTER.print(cassandraManager.readDate(12312L, "test_column_5")
																				   .getTime()));

		// Now lets remove the entire row.
		cassandraManager.deleteRow(12312L);

		assertEquals(new Integer(-1), cassandraManager.readInteger(12312L, "test_column_1", -1));
		assertEquals(null, cassandraManager.readString(12312L, "test_column_3"));
	}

	/**
	 * testUpdateStringWithNoTimestampOrTTL
	 */
	@Test
	public void testUpdateRowScenario12() {

		final String columnFamily = "testUpdateRowScenario12";
		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);
		final long objectId = 1234L;

		cassandraManager.updateString(objectId, "test_column", "test_value", -1, -1);

		final CassandraColumn testColumn = cassandraManager.readColumnInfo(objectId, "test_column");
		assertEquals("test_value", testColumn.getValue());

		compareDates(System.currentTimeMillis(), convertToJavaTimestamp(testColumn.getTimestamp()));
		assertEquals(0, testColumn.getTtl());
		assertEquals("test_column", testColumn.getName());

	}

	private void compareDates(long expected, long actual) {
		Date expectedDate = new Date(expected);
		Date actualDate = new Date(actual);
		assertTrue(DateUtils.isSameDay(expectedDate, actualDate));
	}

	private long convertToJavaTimestamp(long cassandraTimestamp) {
		return cassandraTimestamp / 1000;
	}

	/**
	 * testUpdateStringWithTimestampOnly
	 */
	@Test
	public void testUpdateRowScenario11() {
		String columnFamily = "testUpdateRowScenario11";
		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);

		final long timestamp = new GregorianCalendar(2006, 8, 4).getTimeInMillis();

		cassandraManager.updateString(1234L, "test_column", "test_value", timestamp, -1);

		final CassandraColumn testColumn = cassandraManager.readColumnInfo(1234L, "test_column");
		assertEquals("test_value", testColumn.getValue());
		compareDates(timestamp, testColumn.getTimestamp());
		assertEquals(0, testColumn.getTtl());
		assertEquals("test_column", testColumn.getName());
	}

	/**
	 * testUpdateStringWithTTLOnly
	 */
	@Test
	public void testUpdateRowScenario10() {
		final String columnFamily = "testUpdateRowScenario10";
		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);
		final long objectId = 1234L;

		final int ttl = 2;
		cassandraManager.updateString(objectId, "test_column", "test_value", -1, ttl);

		final long startTime = System.nanoTime();

		// assuming this executed within 3 seconds, we should get a result.
		CassandraColumn testColumn = cassandraManager.readColumnInfo(objectId, "test_column");
		assertEquals("test_value", testColumn.getValue());

		// since we don't know what the set timestamp is, we are leaving the delta to be the time difference between
		// now and when we did the insertion.
		compareDates(System.currentTimeMillis(), convertToJavaTimestamp(testColumn.getTimestamp()));
		assertEquals(ttl, testColumn.getTtl());
		assertEquals("test_column", testColumn.getName());

		// lets wait for ttl and see whether this has expired.
		try {
			Thread.sleep(1000 * (ttl + 1));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


		testColumn = cassandraManager.readColumnInfo(objectId, "test_column");
		assertNull(testColumn);

	}

	/**
	 * testUpdateStringWithBothTimestampAndTTL
	 */
	@Test
	public void testUpdateRowScenario9() {
		final String columnFamily = "testUpdateRowScenario9";
		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);
		final long objectId = 1234L;

		final int ttl = 2;
		cassandraManager.updateString(objectId, "test_column", "test_value", 500, ttl);

		final long startTime = System.nanoTime();

		// assuming this executed within 3 seconds, we should get a result.
		final CassandraColumn testColumn = cassandraManager.readColumnInfo(objectId, "test_column");
		assertEquals("test_value", testColumn.getValue());

		assertEquals(500, testColumn.getTimestamp());
		assertEquals(ttl, testColumn.getTtl());
		assertEquals("test_column", testColumn.getName());
	}

	/**
	 * testUpdateStringWithBothTimpstampsAndTTLSetToNegative
	 */
	@Test
	public void testUpdateRowScenario8() {
		final String columnFamily = "testUpdateRowScenario8";
		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);
		final long objectId = 1234L;

		cassandraManager.updateString(objectId, "test_column", "test_value", -100, -2000);

		final long startTime = System.nanoTime();
		final CassandraColumn testColumn = cassandraManager.readColumnInfo(objectId, "test_column");
		assertEquals("test_value", testColumn.getValue());

		compareDates(System.currentTimeMillis(), convertToJavaTimestamp(testColumn.getTimestamp()));
		assertEquals(0, testColumn.getTtl());
		assertEquals("test_column", testColumn.getName());

	}

	/**
	 * testUpdateRowWithBothTimpStampAndTTLSetToNull
	 */
	@Test
	public void testUpdateRowScenario7() {
		testUpdateRowResults(null, null, "testUpdateRowScenario7");
	}

	/**
	 * testUpdateRowWithBothTimpStampAndTTLSetToEmptyMaps
	 */
	@Test
	public void testUpdateRowScenario6() {
		testUpdateRowResults(new HashMap<String, Long>(), new HashMap<String, Integer>(), "testUpdateRowScenario6");
	}

	private void testUpdateRowResults(Map<String, Long> timestamps, Map<String, Integer> ttls, String columnFamily) {
		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(columnFamily);
		final long objectId = 1234L;

		Map<String, Object> columns = new HashMap<String, Object>();
		for (int i = 0; i < 5; i++) {
			columns.put("Column_" + i, "value_" + i);
		}
		cassandraManager.updateRow(objectId, columns, timestamps, ttls);


		CassandraColumn testColumn = null;
		for (String columnName : columns.keySet()) {
			testColumn = cassandraManager.readColumnInfo(objectId, columnName);
			assertEquals(columnName, testColumn.getName());


			final boolean timeStampAvailable = timestamps != null && timestamps.get(columnName) != null &&
					timestamps.get(columnName) > 0;
			final long expected = timeStampAvailable ? timestamps.get(columnName) : System.currentTimeMillis();
			final long actual = timeStampAvailable ? testColumn.getTimestamp() : convertToJavaTimestamp(
					testColumn.getTimestamp());
			compareDates(expected, actual);


			final boolean ttlAvailable = ttls != null && ttls.get(columnName) != null && ttls.get(columnName) > 0;
			assertEquals(ttlAvailable ? ttls.get(columnName) : 0, testColumn.getTtl());
			assertEquals(columns.get(columnName), testColumn.getValue());
		}
	}

	/**
	 * testUpdateRowWithAllTimpStampsSetButNoTTL
	 */
	@Test
	public void testUpdateRowScenario5() {
		Map<String, Long> timestamps = new HashMap<String, Long>();
		Random random = new Random();
		for (int i = 0; i < 5; i++) {
			timestamps.put("Column_" + i, Math.abs(random.nextLong()));
		}
		testUpdateRowResults(timestamps, null, "testUpdateRowScenario5");
	}

	/**
	 * testUpdateRowWithSomeTimestampsSetButNoTTL
	 */
	@Test
	public void testUpdateRowScenario4() {
		Map<String, Long> timestamps = new HashMap<String, Long>();
		Random random = new Random();

		timestamps.put("Column_2", Math.abs(random.nextLong()));
		timestamps.put("Column_4", Math.abs(random.nextLong()));

		testUpdateRowResults(timestamps, null, "testUpdateRowScenario4");
	}

	/**
	 * testUpdateRowWithAllTTLSetButNoTimestamp
	 */
	@Test
	public void testUpdateRowScenario3() {
		Map<String, Integer> ttls = new HashMap<String, Integer>();

		Random random = new Random();
		for (int i = 0; i < 5; i++) {
			ttls.put("Column_" + i, Math.abs(random.nextInt(TEN_YEARS) + 20));
		}
		testUpdateRowResults(null, ttls, "testUpdateRowScenario3");
	}

	/**
	 * testUpdateRowWithSomeTTLsSetButNoTimestamp
	 */
	@Test
	public void testUpdateRowScenario2() {
		Map<String, Integer> ttls = new HashMap<String, Integer>();
		Random random = new Random();

		ttls.put("Column_1", Math.abs(random.nextInt(TEN_YEARS) + 20));
		ttls.put("Column_3", Math.abs(random.nextInt(TEN_YEARS) + 20));

		testUpdateRowResults(null, ttls, "testUpdateRowScenario2");
	}

	/**
	 * testUpdateRowWithAllTTLAndTimestampSet
	 */
	@Test
	public void testUpdateRowScenario1() {
		Map<String, Integer> ttls = new HashMap<String, Integer>();
		Map<String, Long> timestamps = new HashMap<String, Long>();
		Random random = new Random();
		for (int i = 0; i < 5; i++) {
			ttls.put("Column_" + i, Math.abs(random.nextInt(TEN_YEARS) + 20));
			timestamps.put("Column_" + i, Math.abs(random.nextLong()));
		}
		testUpdateRowResults(timestamps, ttls, "testUpdateRowScenario1");
	}

	@Test
	public void outOfOrderDeleteTest() throws Exception {
		final long SECOND_KEY = 112233L;

		HecubaClientManager<Long> cassandraManager = getHecubaClientManager("testUpdateRowScenario1");

		Map<String, Object> rows = new HashMap<String, Object>();
		rows.put("column1", "value1");
		rows.put("column2", "value2");
		rows.put("column3", "value3");

		// add a new row to CF.
		cassandraManager.updateRow(1234L, rows);

		// delete it and make sure its gone.
		cassandraManager.deleteRow(1234L);
		CassandraResultSet<Long, String> longStringCassandraResultSet = cassandraManager.readAllColumns(1234L);
		assertFalse(longStringCassandraResultSet.hasResults());

		// now add another row to CF with multiple columns.
		HashMap<String, Long> timestampMap = new HashMap<String, Long>();
		long earlierTimestamp = System.currentTimeMillis();
		for (String columnName : rows.keySet()) {
			timestampMap.put(columnName, earlierTimestamp);
		}

		cassandraManager.updateRow(SECOND_KEY, rows, timestampMap, null);

		Thread.sleep(100);

		long timestamp = System.currentTimeMillis();

		// send an update to that row with a new timestamp (say X)
		cassandraManager.updateString(SECOND_KEY, "column1", "NewValue", timestamp, -1);
		CassandraResultSet<Long, String> resultsBeforeDelete = cassandraManager.readAllColumns(SECOND_KEY);
		assertNotNull(resultsBeforeDelete.getString("column2", null));
		assertNotNull(resultsBeforeDelete.getString("column3", null));

		assertTrue(resultsBeforeDelete.hasResults());
		assertEquals("NewValue", resultsBeforeDelete.getString("column1"));

		// send a delete to that row with a timestamp older than X.
		cassandraManager.deleteRow(SECOND_KEY, timestamp - 5);
		CassandraResultSet<Long, String> resultsAfterDelete = cassandraManager.readAllColumns(SECOND_KEY);

		assertTrue(resultsAfterDelete.hasResults());
		assertEquals("NewValue", resultsAfterDelete.getString("column1"));
		assertNull(resultsAfterDelete.getString("column2", null));
		assertNull(resultsAfterDelete.getString("column3", null));

	}

	@Test
	public void testSecondaryIndexMultiGet() {
		String columnFamily = "testSecondaryIndexMultiGet";

		CassandraParamsBean bean = new CassandraParamsBean();
		bean.setClustername(CLUSTER_NAME);
		bean.setLocationURLs(LOCATION);
		bean.setThriftPorts(PORT);
		bean.setKeyspace(KEYSPACE);
		bean.setCf(columnFamily);
		bean.setSiColumns("MySecondaryKey_1:MySecondaryKey_2");

		//Large SecondIndex result set.
		HecubaClientManager<Long> cassandraManager = getHecubaClientManager(bean);

		final int numberOfRecordsInserted = 25;
		//Write 120 rows to cassandra, with the same secondary Index.
		for (long i = 0; i < numberOfRecordsInserted; i++) {
			HashMap<String, Object> row = new HashMap<>();
			row.put("column_1", "value_1");
			row.put("column_2", "value_2");
			row.put("MySecondaryKey_1", i % 5);
			cassandraManager.updateRow(i, row);
		}

		List<String> expectedSecondaryIndexValues = new ArrayList<>();
		expectedSecondaryIndexValues.add("1");
		expectedSecondaryIndexValues.add("2");

		// try to retrieve data by secondaryIndex value.
		CassandraResultSet<Long, String> results = cassandraManager.retrieveBySecondaryIndex("MySecondaryKey_1",
																							 expectedSecondaryIndexValues);
		assertNotNull(results);

		int count = 1;
		while (results.hasNextResult()) {
			results.nextResult();
			count++;
		}
		assertEquals(numberOfRecordsInserted * 2 / 5, count);

		// try to retrieve data by secondaryIndex value.
		results = cassandraManager.retrieveBySecondaryIndex("MySecondaryKey_1", expectedSecondaryIndexValues);

		// check the first one here.
		assertTrue(results.hasResults());
		Long key = results.getKey();
		assertTrue((key % 5) == 1 || (key % 5) == 2);

		while (results.hasNextResult()) {
			results.nextResult();
			key = results.getKey();
			assertTrue((key % 5) == 1 || (key % 5) == 2);
		}


	}

	@Override
	protected List<String> getSecondaryIndexExcludeList() {
		return null;
	}

	@Override
	protected Map<String, String> getColumnValueTypeOverrides() {
		return null;
	}
}
