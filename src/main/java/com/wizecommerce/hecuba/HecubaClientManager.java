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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.wizecommerce.hecuba.util.ConfigUtils;

/**
 * This is a convenience class that will enable a user to interact with Column Families. It is
 * assumed that all the column names are strings.
 * <p/>
 * This is a stateful implementation of the Cassandra manager. The parameters set either in the constructor and/or
 * setter methods will be used to carry our update/read/delete operations.
 * <p/>
 *
 * @author - Eran Chinthaka Withana
 */
public abstract class HecubaClientManager<K> {

	protected String clusterName;
	protected String locationURLs;
	protected String ports;
	protected String keyspace;
	protected String columnFamily;
	protected int maxColumnCount; // used through property if column count in row > 100
	protected int maxSiColumnCount;

	// being a schema flexible datastore we sometimes need to create secondary indexes based on the names of the
	// columns. The following parameter will enable the secondary index columns. If you set this parameter,
	// we will match each and every column name with the given pattern and create a secondary index out of this.
	protected String secondaryIdxByColumnPattern = null;

	protected boolean isClientAdapterDebugMessagesEnabled;


	/**
	 * Secondary Index Implementation ==============================
	 * <p/>
	 * We tried to use Cassandra's in-built secondary indexes but it ended up in tears as these hidden secondary
	 * indexes
	 * sometimes didn't return proper results. Even more than that some of the results were intermittent, meaning the
	 * same request works and not works sometimes.
	 * <p/>
	 * Due to this complication, we decided to maintain our own secondary indexes as of 14th November, 2012.
	 * <p/>
	 * In this implementation, each object has its own column family to store the secondary indexes. During a writing,
	 * we intercept those calls and check whether its a change to the secondary index. If it is, then we will delete
	 * the
	 * previous entry, if any, and create a new entry in the table mapping the new secondary index to the original
	 * object.
	 * <p/>
	 * In the read path, we will provide a new method, retrieveBySecondaryIndex(String columnName, String
	 * value):List&lt;String&gt;, where the list contains the set of ids linking to the main table.
	 * <p/>
	 * Features: + we will be able to have indexes for any number of columns + the implementation of secondary indexes
	 * will be hidden from the client and he/she has to do minimal amount of work to get it working
	 */

	protected List<String> columnsToIndexOnColumnNameAndValue;
	protected boolean isSecondaryIndexByColumnNameAndValueEnabled = false;
	protected boolean isSecondaryIndexesByColumnNamesEnabled = false;

	protected static Logger log = Logger.getLogger(HecubaClientManager.class);

	/**
	 * Creates an instance of the Hecuba client manager to make the calls to Cassandra cluster easier.
	 *
	 * @param parameters --
	 */
	public HecubaClientManager(CassandraParamsBean parameters) {
		this.clusterName = parameters.getClustername();
		this.locationURLs = parameters.getLocationURLs();
		this.ports = parameters.getThriftPorts();
		this.keyspace = parameters.getKeyspace();
		this.columnFamily = parameters.getCf();
		this.maxColumnCount = parameters.getMaxColumnCount();
		this.maxSiColumnCount = parameters.getMaxSiColumnCount();

		if (StringUtils.isNotBlank(parameters.getSiColumns())) {
			columnsToIndexOnColumnNameAndValue = Arrays.asList(StringUtils.split(parameters.getSiColumns(), ":"));
			Collections.sort(columnsToIndexOnColumnNameAndValue);
			isSecondaryIndexByColumnNameAndValueEnabled = true;
		}

		String siByColumnsPattern = parameters.getSiByColumnsPattern();
		if (StringUtils.isNotEmpty(siByColumnsPattern)) {
			this.secondaryIdxByColumnPattern = siByColumnsPattern;
			this.isSecondaryIndexesByColumnNamesEnabled = true;
		}

		init();
	}

	/**
	 * WARNING: Use the following constructor for testing purpose only. Please use {@link
	 * HecubaClientManager#HecubaClientManager(CassandraParamsBean)} for other purposes Creates an instance of
	 * the
	 * manager to make the calls to Cassandra cluster easier. The parameters provided in this constructor can be
	 * changed
	 * later, if needed, using the relevant setter methods. Note that frequent changes to these parameters can degrade
	 * the performance.
	 *
	 * @param clusterName  - name of the cassandra cluster
	 * @param locationURL  - pointer to a node in the cassandra cluster
	 * @param ports        - the port at which the node you are connecting is listening to thrift messages. Default is
	 *                     9106 and this value is printed on the console when you start Cassandra instance.
	 * @param keyspace     - the keyspace you will be managing using this manager.
	 * @param columnFamily - the column family you will be managing using this manager.
	 */
	public HecubaClientManager(String clusterName, String locationURL, String ports, String keyspace,
			String columnFamily) {
		super();
		this.clusterName = clusterName;
		this.locationURLs = locationURL;
		this.ports = ports;
		this.keyspace = keyspace;
		this.columnFamily = columnFamily;

		init();
	}

	/**
	 * Write stuff in init() method which is common to all constructors of this class
	 */
	private void init() {
		this.isClientAdapterDebugMessagesEnabled = ConfigUtils.getInstance().getConfiguration().getBoolean(
				HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".hectorpools.enabledebugmessages", false);
	}

	/**
	 * WARNING: Following constructor is for testing purposes only. Please use {@link
	 * HecubaClientManager#HecubaClientManager()} (String, String, String, String, String,
	 * me.prettyprint.hector.api.Serializer)} or
	 * {@link HecubaClientManager#HecubaClientManager(CassandraParamsBean)} for other purposes.
	 */
	public HecubaClientManager() {

	}

	public void setColumnsToIndexOnColumnNameAndValue(List<String> columnNames) {
		this.columnsToIndexOnColumnNameAndValue = columnNames;
	}

	/**
	 * Updates the value of the column in the given row (identified by the key).
	 *
	 * @param key        - key to identify the row.
	 * @param columnName - column to be updated.
	 * @param value      - value to be inserted.
	 */
	public void updateString(K key, String columnName, String value) {
		updateString(key, columnName, value, -1, -1);
	}

	/**
	 * This method will give the user more power to control what goes into Cassandra. With every column insert, one can
	 * specify four different parameters.
	 * <p/>
	 * <ol> <li>Column name</li> <li>Column value</li> <li>Timestamp - this, if not specified, is set by our client
	 * library. </li> <li>Time To Live (TTL) - this, if not specified, is set to be indefinite.</li> </ol>
	 * <p/>
	 * This method allows one to specify all the above parameters, but @link updateString(K key, String columnName,
	 * String value) will set the defaults to ttl and timestamps.
	 * <p/>
	 * Also, if you want to set only parameters and not care about the other, feel free to set the value to -1 or any
	 * negative value so that we will detect it and set the default values. For example,  you can invoke this method
	 * with updateString(1234, "MyColumn", "MyValue", -1, 22211). This will only set the given ttl but use the client
	 * library to set the timestamp.
	 *
	 * @param key        - key to identify the row.
	 * @param columnName - column to be updated.
	 * @param value      - value to be inserted.
	 * @param timestamp  - timestamp to set. Make sure this is in nanoseconds
	 * @param ttl        - time-to-live for the column so that Cassandra could expire it
	 */
	public abstract void updateString(K key, String columnName, String value, long timestamp, int ttl);

	/**
	 * Updates the value of the column in the given row (identified by the key).
	 *
	 * @param key        - key to identify the row.
	 * @param columnName - column to be updated.
	 * @param value      - value to be inserted.
	 */
	public abstract void updateByteBuffer(K key, String columnName, ByteBuffer value);

	public abstract void createKeyspace(String keyspace);

	public abstract void createKeyspaceAndColumnFamilies(String keyspace, List<ColumnFamilyInfo> columnFamilies);

	/**
	 * Updates the value of the column in the given row (identified by the key).
	 * <p/>
	 * Note that the value will be converted to a String before storing in Cassandra.
	 *
	 * @param key        - key to identify the row.
	 * @param columnName - column to be updated.
	 * @param value      - value to be inserted.
	 */
	public void updateBoolean(K key, String columnName, Boolean value) {
		updateString(key, columnName, value ? "true" : "false");
	}

	/**
	 * Updates the value of the column in the given row (identified by the key).
	 * <p/>
	 * Note that the value will be converted to a String before storing in Cassandra.
	 *
	 * @param key        - key to identify the row.
	 * @param columnName - column to be updated.
	 * @param value      - value to be inserted.
	 */
	public void updateDate(K key, String columnName, Date value) {
		updateString(key, columnName, HecubaConstants.DATE_FORMATTER.print(value.getTime()));
	}

	/**
	 * Updates the value of the column in the given row (identified by the key).
	 * <p/>
	 * Note that the value will be converted to a String before storing in Cassandra.
	 *
	 * @param key        - key to identify the row.
	 * @param columnName - column to be updated.
	 * @param value      - value to be inserted.
	 */
	public void updateDouble(K key, String columnName, Double value) {
		updateString(key, columnName, value.toString());
	}

	/**
	 * Updates the value of the column in the given row (identified by the key).
	 * <p/>
	 * Note that the value will be converted to a String before storing in Cassandra.
	 *
	 * @param key        - key to identify the row.
	 * @param columnName - column to be updated.
	 * @param value      - value to be inserted.
	 */
	public void updateLong(K key, String columnName, Long value) {
		updateString(key, columnName, value.toString());
	}

	/**
	 * Updates the value of the column in the given row (identified by the key).
	 * <p/>
	 * Note that the value will be converted to a String before storing in Cassandra.
	 *
	 * @param key        - key to identify the row.
	 * @param columnName - column to be updated.
	 * @param value      - value to be inserted.
	 */
	public void updateInteger(K key, String columnName, Integer value) {
		updateString(key, columnName, value.toString());
	}

	/**
	 * Removes the keyspace from the cluster.
	 *
	 * @param keyspace - name of the keyspace to be removed.
	 */
	public abstract void dropKeyspace(String keyspace);

	/**
	 * @param columnFamilyName - name of the column family to be created.
	 */
	public abstract void addColumnFamily(String keyspace, String columnFamilyName);

	/**
	 * Removes a column family.
	 *
	 * @param columnFamilyName - name of the column family to be removed.
	 */
	public abstract void dropColumnFamily(String keyspace, String columnFamilyName);

	// ====================================================================================

	/**
	 * Updates a complete row and uses the same timestamp.
	 * <p/>
	 * Note: We are pushing even the columns with null values by putting "null" string to those corresponding columns.
	 * This is useful because if a column currently has a non-null value and if one wants to remove it he should be
	 * able
	 * to set the null value.
	 *
	 * @param key - key of the column to be updated.
	 * @param row - a map of columnNames and their respected values to be updated.
	 */
	public void updateRow(K key, Map<String, Object> row) {
		updateRow(key, row, null, null);
	}

	/**
	 * Updates a complete row and uses the given timestamps and ttl values for each column.
	 * <p/>
	 * If you want to use the default values for any of the columns, either do not add that column name mapping to
	 * timetamps or ttls maps or set the value to a negative value. Setting any of these columns to null will make the
	 * defaults to be applied for all the columns.
	 *
	 * @param key        - key of the row to be updated.
	 * @param row        - a map of column names and their values
	 * @param timestamps - a map of column names to their timestamps. Defaults to the underlying clients timestamp
	 *                   implementation.
	 * @param ttls       - a map of column names to their ttls. Defaults to not expire.
	 */
	public abstract void updateRow(K key, Map<String, Object> row, Map<String, Long> timestamps,
			Map<String, Integer> ttls);

	/**
	 * Reads the value of a column related to a given key.
	 *
	 * @param key        - key of the column to be read.
	 * @param columnName - name of the column to be read.
	 *
	 * @return
	 */
	public abstract String readString(K key, String columnName);

	/**
	 * Retrieves a column together with its name, value, timestamp and ttl.
	 *
	 * @param key
	 * @param columnName
	 *
	 * @return
	 */
	public abstract CassandraColumn readColumnInfo(K key, String columnName);

	/**
	 * Reads the value of a column related to a given key and returns it as a boolean.
	 * <p/>
	 * Note: since all the values are stored as Strings this method will explicitly convert the string to a boolean.
	 *
	 * @param key        - key of the column to be read.
	 * @param columnName - name of the column to be read.
	 *
	 * @return
	 */
	public Boolean readBoolean(K key, String columnName) {
		return readBoolean(key, columnName, false);
	}

	/**
	 * Reads the value of a column related to a given key and returns it as a boolean.
	 * <p/>
	 * Note: since all the values are stored as Strings this method will explicitly convert the string to a boolean.
	 *
	 * @param key        - key of the column to be read.
	 * @param columnName - name of the column to be read.
	 *
	 * @return
	 */
	public Boolean readBoolean(K key, String columnName, boolean defaultValue) {
		final String value = readString(key, columnName);
		return value == null ? defaultValue : ("true".equalsIgnoreCase(value) || "1".equals(value));
	}

	public Date readDate(K key, String columnName) {
		return readDate(key, columnName, null);
	}

	public Date readDate(K key, String columnName, Date defaultDate) {

		final String value = readString(key, columnName);
		return value == null ? defaultDate : HecubaConstants.DATE_FORMATTER.parseDateTime(value).toDate();

	}

	public Integer readInteger(K key, String columnName) {
		return readInteger(key, columnName, null);
	}

	public Integer readInteger(K key, String columnName, Integer defaultInt) {
		final String value = readString(key, columnName);
		return value == null ? defaultInt : Integer.parseInt(value);
	}

	public Long readLong(K key, String columnName) {
		return readLong(key, columnName, -1);
	}

	public Long readLong(K key, String columnName, long defaultLong) {
		final String value = readString(key, columnName);
		return value == null ? defaultLong : Long.parseLong(value);
	}

	public Double readDouble(K key, String columnName) {
		return readDouble(key, columnName, -1.0);
	}

	public Double readDouble(K key, String columnName, double defaultDouble) {
		final String value = readString(key, columnName);
		return value == null ? defaultDouble : Double.parseDouble(value);
	}

	/**
	 * Retrieves all the columns related to a given key.
	 * Analogous to "Select * from TABLE" in SQL world.
	 * <p/>
	 * If column family can contain more than 100 columns, set the maxColumnCount
	 * to the max no of columns possible in the column family row.
	 * Otherwise only 100 columns would be fetched.
	 *
	 * @param key - key of the column to be read.
	 *
	 * @return CassandraResultSet (interface to get column values)
	 */
	public abstract CassandraResultSet<K, String> readAllColumns(K key) throws Exception;


	/**
	 * Retrieves set of columns (within specified range) for the key
	 *
	 * @param key      - key of the column family row
	 * @param start    - column name marking the start of range (null for no boundary on start)
	 * @param end      - column name marking the end of range (null for no boundary on end)
	 * @param reversed - whether the results should be ordered in reversed order (Similar to ORDER BY blah DESC in
	 *                 SQL).
	 *                 When reversed is true, start will determine the right end of the range while finish will
	 *                 determine the left,
	 *                 meaning start must be >= finish.
	 * @param count    - number of columns to return (Analogous to "limit X" in SQL world).
	 *
	 * @return CassandraResultSet (interface to get column values) </br>
	 *         If key doesn't exist in Cassandra, returned ResultSet is empty (i.e. no columns present in the result
	 *         set)
	 */
	public abstract CassandraResultSet<K, String> readColumnSlice(K key, String start, String end, boolean reversed,
			int count);

	/**
	 * Retrieves all columns (upto a limit of 10000 columns) for the key. </br>
	 * If number of columns is very large (say 1M), fetching all of them can cause memory issues
	 * and for that pagination should be used.
	 *
	 * @param key - key of the column family row
	 *
	 * @return CassandraResultSet (interface to iterate the columns to get column values) </br>
	 *         If key doesn't exist in Cassandra, returned ResultSet is empty (i.e. no columns present in the result
	 *         set)
	 */
	public CassandraResultSet<K, String> readColumnSliceAllColumns(K key) {
		return readColumnSlice(key, null, null, false, 10000);
	}

	/**
	 * Get the value of a counter and returns 0 if the counter is not available.
	 *
	 * @param key
	 * @param counterColumnName
	 *
	 * @return
	 */
	public abstract Long getCounterValue(K key, String counterColumnName);

	/**
	 * Update the counter with the given value (which can be either negative or positive).
	 * If the counter does not exist, a new counter will be created with the given value.
	 *
	 * @param key
	 * @param counterColumnName
	 * @param value
	 */
	public abstract void updateCounter(K key, String counterColumnName, long value);

	/**
	 * Increase the counter by 1.
	 * If the counter does not exist, a counter with the default value 0 will be created and then incremented.
	 *
	 * @param key
	 * @param counterColumnName
	 */
	public abstract void incrementCounter(K key, String counterColumnName);

	/**
	 * Decrease the counter by 1.
	 * If the counter does not exist, a counter with the default value 0 will be created and then decremented.
	 *
	 * @param key
	 * @param counterColumnName
	 */
	public abstract void decrementCounter(K key, String counterColumnName);

	/**
	 * Retrieves all the columns for the list of keys. In the implementation, check for Id (eg. TAG_ID) column to
	 * determine if the record exists (and put the object in the map).
	 * If column family can contain more than 100 columns, set the maxColumnCount
	 * to the max no of columns possible in the column family row.
	 * Otherwise only 100 columns would be fetched.
	 *
	 * @param keys - Set of keys to retrieve
	 *
	 * @return -- CassandraResultSet<K, String>. CassandraResultSet wraps the object returned by Hector/Astyanax API.
	 *
	 * @throws Exception
	 */
	public abstract CassandraResultSet<K, String> readAllColumns(Set<K> keys) throws Exception;


	/**
	 * Retrieves set of columns (within specified range) for the each key
	 *
	 * @param keys     - set of keys for which columns need to be retrieved
	 * @param start    - column name marking the start of range (null for no boundary on start)
	 * @param end      - column name marking the end of range (null for no boundary on end)
	 * @param reversed - whether the results should be ordered in reversed order (Similar to ORDER BY blah DESC in
	 *                 SQL).
	 *                 When reversed is true, start will determine the right end of the range while finish will
	 *                 determine the left,
	 *                 meaning start must be >= finish.
	 * @param count    - number of columns to return (Analogous to "limit X" in SQL world).
	 *
	 * @return CassandraResultSet (interface to get column values) </br>
	 *         If any key(s) doesn't exist in Cassandra, returned ResultSet will contain no column for those key(s).
	 */
	public abstract CassandraResultSet<K, String> readColumnSlice(Set<K> keys, String start, String end,
			boolean reversed, int count);

	/**
	 * Retrieves all columns (upto a limit of 10000 columns) for set of keys. </br>
	 * If number of columns is very large (say 1M), fetching all of them can cause memory issues
	 * and for that pagination should be used.
	 *
	 * @param keys - set of keys for which all the columns need to be retrieved
	 *
	 * @return CassandraResultSet (interface to iterate the columns to get column values) </br>
	 *         If any key(s) doesn't exist in Cassandra, returned ResultSet will contain no column for those key(s).
	 */
	public CassandraResultSet<K, String> readColumnSliceAllColumns(Set<K> keys) {
		return readColumnSlice(keys, null, null, false, 10000);
	}


	/**
	 * Retrieves only the set of column values.
	 * This can be helpful where the user needs only few columns instead of reading all columns (say 100) stored in a
	 * row
	 *
	 * @param key - key of the column family row
	 *
	 * @return
	 *
	 * @throws Exception
	 */
	public abstract CassandraResultSet<K, String> readColumns(K key, List<String> columnNames) throws Exception;


	/**
	 * Retrieves only the set of column values for given keys.
	 * This can be helpful where the user needs only few columns instead of reading all columns in given rows.
	 *
	 * @param keys        - Set of keys for which specified columns need to be retrieved
	 * @param columnNames - List of column names
	 *
	 * @return
	 *
	 * @throws Exception
	 */
	public abstract CassandraResultSet<K, String> readColumns(Set<K> keys, List<String> columnNames) throws Exception;

	/**
	 * Deletes a given column value of a row identified by the key.
	 *
	 * @param key        - key of the row.
	 * @param columnName - name of the column to be deleted.
	 */
	public abstract void deleteColumn(K key, String columnName);

	/**
	 * Deletes a given column value of a row identified by the key.
	 *
	 * @param key            - key of the row.
	 * @param columnNameList - a list that contains name of the columns to be deleted.
	 */
	public abstract void deleteColumns(K key, List<String> columnNameList);

	/**
	 * Deletes an entire row for a given key.
	 *
	 * @param key - the key of the row to be deleted.
	 */
	public abstract void deleteRow(K key);

	public abstract void deleteRow(K key, long timestamp);

	// ====================================================
	// Secondary Index Related Methods
	// ====================================================

	public abstract CassandraResultSet readAllColumnsBySecondaryIndex(Map<String, String> parameters, int limit);

	/**
	 * We are indexing based on the name of the column AND the value stored under that column. For example,
	 * if we have a person column family, keyed by person id/name, we might also want to retrieve the people by the
	 * department they belong to. In that case, we should be creating a secondary index on the name and the value of
	 * department column.
	 *
	 * @param columnName  : name of the column to
	 * @param columnValue : value of the column
	 *
	 * @return
	 */
	public abstract CassandraResultSet<K, String> retrieveBySecondaryIndex(String columnName, String columnValue);

	/**
	 * We are indexing based on the name of the column AND the value stored under that column. For example,
	 * if we have a person column family, keyed by person id/name, we might also want to retrieve the people by the
	 * department they belong to. In that case, we should be creating a secondary index on the name and the value of
	 * department column.
	 *
	 * This method will allow to do a multi get based on the secondary index.
	 *
	 * @param columnName
	 * @param columnValue
	 *
	 * @return
	 */
	public abstract CassandraResultSet<K, String> retrieveBySecondaryIndex(String columnName, List<String> columnValue);

	/**
	 * There are scenarios where we need to create secondary indexes only on the name of the column name. This
	 * especially happens if we encode some information in the column name itself. For example,
	 * we could set the name of the column to be the stocks a person had purchased whereas the value corresponds to
	 * the specific details of stock purchase. At one point of time if someone wants to find out the people
	 * associated with a stock, we can easily do that by creating a secondary index on the stock.
	 *
	 * @param columnName : name of the column to be retrieved
	 *
	 * @return
	 */
	public abstract CassandraResultSet<K, String> retrieveByColumnNameBasedSecondaryIndex(String columnName);

	/**
	 * We are indexing based on the name of the column AND the value stored under that column.
	 * Sometimes there are use cases to only retrieve keys (and not all columns) using secondary index.
	 * Furthermore, after retrieving keys, client can retrieve only specific columns using {@link HecubaClientManager#readColumns(Object, List)}
	 * <p/>
	 * {@link HecubaClientManager#retrieveBySecondaryIndex(String, String)} returns {@link CassandraResultSet} by reading allColumns
	 * 
	 * @param columnName - Cassandra column name which is secondary indexed
	 * @param columnValue - Cassandra column value which is secondary indexed
	 * @return ordered list of keys
	 */
	public abstract List<K> retrieveKeysBySecondaryIndex(String columnName, String columnValue);

	/**
	 * We are indexing based on the name of the column AND the value stored under that column.
	 * Retrieve keys using secondary index for multiple column values.
	 * This is a multi-get for {@link HecubaClientManager#retrieveKeysBySecondaryIndex(String, String)}
	 * <p/>
	 * {@link HecubaClientManager#retrieveBySecondaryIndex(String, List)} returns result by reading allColumns

	 * @param columnName - Cassandra column name which is secondary indexed
	 * @param columnValues - list of cassandra column values which are secondary indexed
	 * @return map of columnValue to list of keys for that column value
	 */
	public abstract Map<String, List<K>> retrieveKeysBySecondaryIndex(String columnName, List<String> columnValues);

	/**
	 * There are scenarios where we need to create secondary indexes only on the name of the column name.
	 * Sometimes there are use cases to only retrieve keys (and not all columns) using secondary index.
	 * Furthermore, after retrieving keys, client can retrieve only specific columns using {@link HecubaClientManager#readColumns(Object, List)}
	 * <p/>
	 * {@link HecubaClientManager#retrieveByColumnNameBasedSecondaryIndex(String)} returns result by reading allColumns
	 * @param columnName
	 * @return
	 */
	public abstract List<K> retrieveKeysByColumnNameBasedSecondaryIndex(String columnName);
	
	/**
	 * Sometimes we need all secondary keys based on a particular column. Unfortunately there is no way to get a range of secondary keys
	 * unless we use an ordered partitioner. This is an alternate API to get all secondary keys, filtering is left to the user.
	 * This call can be costly depending upon the amount of data in CF and should be avoided if other approaches are available.
	 * <p/>
	 * {@link HecubaClientManager#retrieveAllSecondaryKeys(int)} returns result by reading all secondary keys.
	 * @param limit 
	 * @return returns all keys or upto <b>limit</b> whichever is less, 
	 * 		   returns null if secondaryIndexColumn is not enabled on this column family.

	 */
	public abstract List<String> retrieveAllSecondaryKeys(int limit);


	// ====================================================
	// Utils
	// ====================================================
	protected String getListOfNodesAndPorts(String locationURLs, String ports) {
		final String paramSeparator = ConfigUtils.getInstance().getConfiguration().getString(
				HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".hecuba.path.separator", ":");
		final String[] splittedPorts = ports.split(paramSeparator);
		final String[] splittedLocationURLs = locationURLs.split(paramSeparator);
		final StringBuffer listOfNodesAndPortsBuffer = new StringBuffer();
		for (int index = 0; index < splittedLocationURLs.length; index++) {
			final String locationURL = splittedLocationURLs[index];
			String port = "";
			if (index < splittedPorts.length) {
				port = splittedPorts[index];
			}

			if (port == null || "".equals(port)) {
				port = "9160";
			}
			listOfNodesAndPortsBuffer.append(locationURL).append(":").append(port).append(",");
		}
		return listOfNodesAndPortsBuffer.substring(0, listOfNodesAndPortsBuffer.lastIndexOf(","));
	}

	// ====================================================
	// Getters and Setters
	// ====================================================

	/**
	 * @return the clusterName
	 */
	public String getClusterName() {
		return clusterName;
	}

	/**
	 * @param clusterName the clusterName to set
	 */
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	/**
	 * @return the locationURL
	 */
	public String getLocationURL() {
		return locationURLs;
	}

	/**
	 * @param locationURL the locationURL to set
	 */
	public void setLocationURL(String locationURL) {
		this.locationURLs = locationURL;
	}

	/**
	 * @return the port
	 */
	public String getPort() {
		return ports;
	}

	/**
	 * @return the keyspace
	 */
	public String getKeyspace() {
		return keyspace;
	}

	/**
	 * @return the columnFamily
	 */
	public String getColumnFamilyName() {
		return columnFamily;
	}

	/**
	 * @param columnFamily the columnFamily to set
	 */
	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public List<String> getColumnsToIndexOnColumnNameAndValue() {
		return columnsToIndexOnColumnNameAndValue;
	}

	public String getSecondaryIndexKey(String columnName, String columnValue) {
		return columnName + ":" + columnValue;
	}

	protected String getSecondaryIndexedColumnValue(String secondaryIndexKey) {
		if (secondaryIndexKey.length() > secondaryIndexKey.indexOf(":") + 1) {
			return secondaryIndexKey.substring(secondaryIndexKey.indexOf(":") + 1, secondaryIndexKey.length());
		} else {
			return "";
		}
	}

	protected List<String> getSecondaryIndexKeys(String columnName, List<String> columnValues) {
		List<String> secondaryIndexKeys = null;
		if (columnValues != null && columnValues.size() > 0) {
			secondaryIndexKeys = new ArrayList<>();
			for (String columnValue : columnValues) {
				secondaryIndexKeys.add(getSecondaryIndexKey(columnName, columnValue));
			}
		}

		return secondaryIndexKeys;
	}

	protected boolean isSecondaryIndexByColumnNameEnabledForColumn(String columnName) {
		return isSecondaryIndexesByColumnNamesEnabled && StringUtils.isNotEmpty(columnName) && columnName.matches(
				secondaryIdxByColumnPattern);
	}

	protected boolean isSecondaryIndexByColumnNameAndValueEnabledForColumn(String columnName) {
		return isSecondaryIndexByColumnNameAndValueEnabled && Collections.binarySearch(
				columnsToIndexOnColumnNameAndValue, columnName) >= 0;
	}
	
	protected abstract void logDownedHosts();
}
