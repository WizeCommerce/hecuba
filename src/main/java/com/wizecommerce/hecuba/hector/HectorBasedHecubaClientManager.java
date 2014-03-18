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

/**
 *
 */
package com.wizecommerce.hecuba.hector;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.cassandra.connection.HConnectionManager;
import me.prettyprint.cassandra.model.IndexedSlicesQuery;
import me.prettyprint.cassandra.model.thrift.ThriftCounterColumnQuery;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ColumnFamilyTemplate;
import me.prettyprint.cassandra.service.template.ColumnFamilyUpdater;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ConsistencyLevelPolicy;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.beans.OrderedRows;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.exceptions.HectorException;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.MutationResult;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.MultigetSliceQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.google.common.base.Joiner;
import com.wizecommerce.hecuba.CassandraColumn;
import com.wizecommerce.hecuba.CassandraParamsBean;
import com.wizecommerce.hecuba.CassandraResultSet;
import com.wizecommerce.hecuba.ColumnFamilyInfo;
import com.wizecommerce.hecuba.HecubaClientManager;
import com.wizecommerce.hecuba.HecubaConstants;
import com.wizecommerce.hecuba.util.ConfigUtils;

/**
 * Configuring Hector Clients:
 * <p/>
 * HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".hectorpools.enable com.nextag.db.cassandra.hectorpools.exhaustedpolicy
 * HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".hectorpools.loadbalancingpolicy com.nextag.db.cassandra.hectorpools
 * .maxactive
 * HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".hectorpools.maxidle com.nextag.db.cassandra.hectorpools.retrydownedhosts
 * HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".hectorpools.retrydownedhostsinseconds
 * HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".hectorpools.thriftsockettimeout
 * HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".hectorpools.usethriftframedtransport
 *
 * @author Eran Chinthaka Withana
 */

public class HectorBasedHecubaClientManager<K> extends HecubaClientManager<K> {
	private List<ColumnFamilyTemplate<K, String>> columnFamilyTemplates;
	private ConsistencyLevelPolicy consistencyLevel;
	private Keyspace keysp;
	private Cluster cluster;
	private HectorClientConfiguration hectorClientConfiguration;
	protected Serializer<K> keySerializer;
	protected ThriftColumnFamilyTemplate<String, K> secondaryIndexedColumnFamilyTemplate;

	public HectorBasedHecubaClientManager(String clusterName, String locationURL, String ports, String keyspace,
			String columnFamily, Serializer<K> keySerializer, boolean enableHectorPools) {
		super(clusterName, locationURL, ports, keyspace, columnFamily);
		init(keySerializer);
	}

	public HectorBasedHecubaClientManager(CassandraParamsBean parameters, Serializer<K> keySerializer,
			boolean enableHectorPools) {
		super(parameters);
		init(keySerializer);
	}

	private void init(Serializer<K> keySerializer) {
		this.keySerializer = keySerializer;
		consistencyLevel = new HectorConsistencyPolicy(columnFamily);

		configureHectorSpecificProperties();
		reConfigureParameters();
	}


	/**
	 * WARNING: Following constructors are for testing purposes only. Please use {@link
	 * HectorBasedHecubaClientManager (String, String, String, String, String, me.prettyprint.hector.api.Serializer)}
	 * for
	 * other purposes.
	 */
	public HectorBasedHecubaClientManager() {
		super();
	}

	public HectorBasedHecubaClientManager(CassandraParamsBean parameters) {
		super(parameters);
	}

	/**
	 * Updates the value of the column in the given row (identified by the key).
	 *
	 * @param key        - key to identify the row.
	 * @param columnName - column to be updated.
	 * @param value      - value to be inserted.
	 */
	public void updateString(K key, String columnName, String value) {

		// update the secondary index, if needed
		updateSecondaryIndexes(key, columnName, value, -1, -1);

		final ColumnFamilyTemplate<K, String> columnFamilyLocal = getColumnFamily();
		final ColumnFamilyUpdater<K, String> updater = columnFamilyLocal.createUpdater(key);
		updater.setString(columnName, value);
		columnFamilyLocal.update(updater);


	}

	public void updateString(K key, String columnName, String value, long timestamp, int ttl) {

		// update the secondary index, if needed
		updateSecondaryIndexes(key, columnName, value, timestamp, ttl);

		try {
			final Mutator<K> mutator = HFactory.createMutator(keysp, keySerializer);

			addInsertionToMutator(key, columnName, value, timestamp, ttl, mutator);

			final MutationResult mutationResult = mutator.execute();

			if (log.isDebugEnabled()) {
				log.debug("Inserted the column " + columnName + " with the value " + value + " at time " + timestamp +
						". Exec Time = " + mutationResult.getExecutionTimeMicro() + ", Host used = " +
						mutationResult.getHostUsed());


			}
		} catch (Exception e) {
			log.warn(ExceptionUtils.getFullStackTrace(e));
		}


	}

	private void updateSecondaryIndexes(K key, String columnName, String value, long timestamp, int ttl) {
		// update the column name and value based secondary index, if needed
		if (isSecondaryIndexByColumnNameAndValueEnabled) {
			updateSecondaryIndexColumn(key, columnName, value, timestamp, ttl);
		}

		// update the column name based secondary index, if needed
		if (isSecondaryIndexesByColumnNamesEnabled) {
			updateSecondaryIndexByColumNames(key, columnName, timestamp, ttl);
		}
	}

	private void addInsertionToMutator(K key, String columnName, String value, long timestamp, int ttl,
			Mutator<K> mutator) {
		if (ttl > 0) {
			mutator.addInsertion(key, columnFamily, HFactory.createColumn(columnName, value, timestamp > 0 ? timestamp :
				keysp.createClock(), ttl, StringSerializer.get(), StringSerializer.get()));
		} else {
			mutator.addInsertion(key, columnFamily, HFactory.createColumn(columnName, value, timestamp > 0 ? timestamp :
				keysp.createClock(), StringSerializer.get(), StringSerializer.get()));
		}
	}

	/**
	 * Updates the value of the column in the given row (identified by the key).
	 *
	 * @param key        - key to identify the row.
	 * @param columnName - column to be updated.
	 * @param value      - value to be inserted.
	 */
	public void updateByteBuffer(K key, String columnName, ByteBuffer value) {
		final ColumnFamilyTemplate<K, String> columnFamilyLocal = getColumnFamily();
		final ColumnFamilyUpdater<K, String> updater = columnFamilyLocal.createUpdater(key);
		updater.setByteBuffer(columnName, value);
		columnFamilyLocal.update(updater);
	}

	/**
	 * Creates a keyspace within the cluster.
	 *
	 * @param keyspace - name of the keyspace to be created.
	 */
	public void createKeyspace(String keyspace) {
		final ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace, "DynCf");
		cluster.addKeyspace(new ThriftKsDef(keyspace, "org.apache.cassandra.locator.SimpleStrategy", 1, Arrays.asList(
				cfDef)));
	}

	/**
	 * Removes the keyspace from the cluster.
	 *
	 * @param keyspace - name of the keyspace to be removed.
	 */
	public void dropKeyspace(String keyspace) {
		cluster.dropKeyspace(keyspace, true);
	}

	/**
	 * @param columnFamilyName - name of the column family to be created.
	 */
	public void addColumnFamily(String keyspace, String columnFamilyName) {
		// First check whether we already have this colum family.
		KeyspaceDefinition keyspaceDescription = cluster.describeKeyspace(keyspace);
		if (keyspaceDescription == null) {
			createKeyspace(keyspace);
		}
		keyspaceDescription = cluster.describeKeyspace(keyspace);
		List<ColumnFamilyDefinition> cfDefs = keyspaceDescription.getCfDefs();
		if (cfDefs != null) {
			for (ColumnFamilyDefinition cfDef : cfDefs) {
				if (cfDef.getName().equals(columnFamilyName)) {
					return;
				}
			}
		}

		final ColumnFamilyDefinition cfDef = HFactory.createColumnFamilyDefinition(keyspace, columnFamilyName);
		cluster.addColumnFamily(cfDef);
	}

	/**
	 * Removes a column family.
	 *
	 * @param columnFamilyName - name of the column family to be removed.
	 */
	public void dropColumnFamily(String keyspace, String columnFamilyName) {
		cluster.dropColumnFamily(keyspace, columnFamilyName);
	}

	/**
	 * Deletes a given column value of a row identified by the key.
	 *
	 * @param key        - key of the row.
	 * @param columnName - name of the column to be deleted.
	 */
	public void deleteColumn(K key, String columnName) {
		final ColumnFamilyTemplate<K, String> columnFamily1 = getColumnFamily();
		final HColumn<String, String> oldValue = readColumn(key, columnName);

		// is this insertion involves a change to secondary indexes
		if (isSecondaryIndexByColumnNameAndValueEnabled && Collections.binarySearch(columnsToIndexOnColumnNameAndValue,
				columnName) >= 0 &&
				oldValue != null) {
			secondaryIndexedColumnFamilyTemplate.deleteColumn(getSecondaryIndexKey(columnName, oldValue.getValue()),
					key);
		}

		if (isSecondaryIndexesByColumnNamesEnabled && columnName.matches(secondaryIdxByColumnPattern)) {
			secondaryIndexedColumnFamilyTemplate.deleteColumn(getSecondaryIndexKey(columnName, ""), key);
		}

		columnFamily1.deleteColumn(key, columnName);
	}

	/**
	 * Deletes an entire row for a given key.
	 *
	 * @param key - the key of the row to be deleted.
	 */
	public void deleteRow(K key) {
		deleteRow(key, -1);
	}

	public void deleteRow(K key, long timestamp) {
		final ColumnFamilyTemplate<K, String> columnFamily1 = getColumnFamily();

		timestamp = timestamp > 0 ? timestamp : keysp.createClock();

		if (isSecondaryIndexByColumnNameAndValueEnabled || isSecondaryIndexesByColumnNamesEnabled) {

			final Mutator<String> secondaryIndexMutator = HFactory.createMutator(keysp, StringSerializer.get());
			String secondaryIndexCF = secondaryIndexedColumnFamilyTemplate.getColumnFamily();

			if (isSecondaryIndexByColumnNameAndValueEnabled) {
				// first read the old values of columns.
				final CassandraResultSet<K, String> oldValues = readColumns(key, columnsToIndexOnColumnNameAndValue);
				for (String columnName : columnsToIndexOnColumnNameAndValue) {
					secondaryIndexMutator.addDeletion(getSecondaryIndexKey(columnName, oldValues.getString(columnName)),
							secondaryIndexCF, key, keySerializer, timestamp);
				}
			}

			if (isSecondaryIndexesByColumnNamesEnabled) {
				final CassandraResultSet<K, String> oldValues = readAllColumns(key);
				for (String columnName : oldValues.getColumnNames()) {
					if (columnName.matches(secondaryIdxByColumnPattern)) {
						secondaryIndexMutator.addDeletion(getSecondaryIndexKey(columnName, ""), secondaryIndexCF, key,
								keySerializer, timestamp);
					}
				}
			}
			secondaryIndexMutator.execute();
		}

		// now delete the row from main CF
		Mutator<K> mutator = HFactory.createMutator(keysp, keySerializer);
		mutator.addDeletion(key, columnFamily, null, StringSerializer.get(), timestamp).execute();
	}

	@Override
	public void updateRow(K key, Map<String, Object> row, Map<String, Long> timestamps, Map<String, Integer> ttls) {
		final Mutator<K> m = HFactory.createMutator(keysp, keySerializer);

		List<String> secondaryColumnsChanged = null;
		List<String> secondaryIndexByColumnNameChanges = null;

		// check whether we have to set the timestamps.
		final boolean timestampsDefined = timestamps != null;

		// check whether we have to set the ttls.
		final boolean ttlsDefined = ttls != null;

		for (String columnName : row.keySet()) {
			final Object value = row.get(columnName);

			String valueToInsert = "null";

			if (value != null) {

				if (value instanceof Integer || value instanceof Long || value instanceof Double) {
					valueToInsert = value.toString();
				} else if (value instanceof Date) {
					valueToInsert = HecubaConstants.DATE_FORMATTER.print(((Date) value).getTime());
				} else if (value instanceof Boolean) {
					valueToInsert = ((Boolean) value) ? "true" : "false";
				} else if (value instanceof String) {
					valueToInsert = (String) value;
				} else {
					// TODO:Eran
					// not sure what to do here. There has to be a serializer to
					// send this value.
					valueToInsert = value.toString();
				}
			}

			addInsertionToMutator(key, columnName, valueToInsert, timestampsDefined && timestamps.get(columnName) !=
					null ? timestamps.get(columnName) : -1, ttlsDefined && ttls.get(columnName) != null ? ttls.get(
							columnName) : -1, m);

			// is this insertion involves a change to secondary indexes
			if (isSecondaryIndexByColumnNameAndValueEnabled) {
				if (Collections.binarySearch(columnsToIndexOnColumnNameAndValue, columnName) >= 0) {

					// doing a late initialization here.
					if (secondaryColumnsChanged == null) {
						secondaryColumnsChanged = new ArrayList<String>();
					}

					secondaryColumnsChanged.add(columnName);
				}
			}

			// is this insertion involves changes to secondary index by column names.
			if (isSecondaryIndexByColumnNameEnabledForColumn(columnName)) {

				// doing a late initialization here.
				if (secondaryIndexByColumnNameChanges == null) {
					secondaryIndexByColumnNameChanges = new ArrayList<String>();
				}
				secondaryIndexByColumnNameChanges.add(columnName);
			}


		}

		if (isSecondaryIndexByColumnNameAndValueEnabled && secondaryColumnsChanged != null &&
				secondaryColumnsChanged.size() > 0) {
			updateSecondaryIndexes(key, row, secondaryColumnsChanged, ttls);
		}

		if (isSecondaryIndexesByColumnNamesEnabled && secondaryIndexByColumnNameChanges != null &&
				secondaryIndexByColumnNameChanges.size() > 0) {
			updateColumnNameBasedSecondaryIndices(key, secondaryIndexByColumnNameChanges, ttls);
		}


		try {
			final MutationResult mutationResult = m.execute();
			log.debug("Row Inserted into Cassandra. Exec Time = " + mutationResult.getExecutionTimeMicro() +
					", Host used = " + mutationResult.getHostUsed());


		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	private void updateColumnNameBasedSecondaryIndices(K key, List<String> secondaryIndexByColumnNameChanges, Map<String, Integer> ttls) {

		final Mutator<String> secondaryIndexMutator = HFactory.createMutator(keysp, StringSerializer.get());
		String secondaryIndexCF = secondaryIndexedColumnFamilyTemplate.getColumnFamily();

		boolean ttlsDefined = MapUtils.isNotEmpty(ttls);

		for (String secondaryIndexByColumnName : secondaryIndexByColumnNameChanges) {
			int ttl = -1;
			if (ttlsDefined && ttls.get(secondaryIndexByColumnName) != null) {
				ttl = ttls.get(secondaryIndexByColumnName);
			}
			HColumn<K, K> hColumn;
			if (ttl > 0) {
				hColumn = HFactory.createColumn(key, key, keysp.createClock(), ttl, keySerializer, keySerializer);
			} else {
				hColumn = HFactory.createColumn(key, key, keysp.createClock(), keySerializer, keySerializer);
			}
			secondaryIndexMutator.addInsertion(getSecondaryIndexKey(secondaryIndexByColumnName, ""), secondaryIndexCF, hColumn);
		}
		MutationResult execute = secondaryIndexMutator.execute();
		log.debug(secondaryIndexByColumnNameChanges.size() + " secondary Indexes got updated for the key " + key.toString() + 
				". Exec Time = " + execute.getExecutionTimeMicro() + ", Host used = " + execute.getHostUsed());

	}

	protected void updateSecondaryIndexes(K key, Map<String, Object> allColumnsToBeChanged,
			List<String> secondaryColumnsChanged, Map<String, Integer> ttls) {

		final Mutator<String> secondaryIndexMutator = HFactory.createMutator(keysp, StringSerializer.get());
		String secondaryIndexCF = secondaryIndexedColumnFamilyTemplate.getColumnFamily();

		// first retrieve the values of these columns from the current row.
		if (secondaryColumnsChanged.size() == 1) {
			String columnName = secondaryColumnsChanged.get(0);
			String oldValue = readString(key, columnName);
			log.info("Updating secondary index for Key = " + key + " column = " + columnName);
			prepareMutatorForSecondaryIndexUpdate(key, allColumnsToBeChanged, secondaryIndexMutator, secondaryIndexCF,
					columnName, oldValue, ttls);
		} else {
			CassandraResultSet<K, String> oldValues = readColumns(key, secondaryColumnsChanged);
			for (String columnName : secondaryColumnsChanged) {
				String oldValue = oldValues.getString(columnName);
				prepareMutatorForSecondaryIndexUpdate(key, allColumnsToBeChanged, secondaryIndexMutator,
						secondaryIndexCF, columnName, oldValue, ttls);
			}
		}


		MutationResult execute = secondaryIndexMutator.execute();
		log.debug(secondaryColumnsChanged.size() + " secondary Indexes got updated for the key " + key.toString() +
				". Exec Time = " +
				execute.getExecutionTimeMicro() + ", Host used = " + execute.getHostUsed());
	}

	private void prepareMutatorForSecondaryIndexUpdate(K key, Map<String, Object> allColumnsToBeChanged,
			Mutator<String> secondaryIndexMutator, String secondaryIndexCF,
			String columnName, String oldValue, Map<String, Integer> ttls) {
		if (!StringUtils.isBlank(oldValue) && !"null".equalsIgnoreCase(oldValue)) {
			// delete those indexes first
			secondaryIndexMutator.addDeletion(getSecondaryIndexKey(columnName, oldValue), secondaryIndexCF, key,
					keySerializer);
		}

		int ttl = -1;
		if (ttls != null && ttls.get(columnName) != null) {
			ttl = ttls.get(columnName);
		}

		// add the new ones
		if (ttl > 0) {
			secondaryIndexMutator.addInsertion(getSecondaryIndexKey(columnName, allColumnsToBeChanged.get(columnName)
					.toString()), secondaryIndexCF, HFactory.createColumn(key, key, keysp.createClock(), ttl, keySerializer, keySerializer));
		} else {
			secondaryIndexMutator.addInsertion(getSecondaryIndexKey(columnName, allColumnsToBeChanged.get(columnName)
					.toString()), secondaryIndexCF, HFactory.createColumn(key, key, keysp.createClock(), keySerializer, keySerializer));
		}
	}

	private void updateSecondaryIndexByColumNames(K key, String columnName, long timestamp, int ttl) {
		// here, there is nothing to retrieve first and delete because the secondary index is based on the name of the
		// column and not on the value.
		if (isSecondaryIndexByColumnNameEnabledForColumn(columnName)) {
			updateSecondaryIndexColumnFamily(key, columnName, "", timestamp, ttl, "");
		}
	}

	protected void updateSecondaryIndexColumn(K key, String columnName, String columnValue, long timestamp, int ttl) {

		if (Collections.binarySearch(columnsToIndexOnColumnNameAndValue, columnName) >= 0) {

			// first retrieve the old value of this column to delete it from the secondary index.
			HColumn<String, String> oldColumn = getColumnFamily().querySingleColumn(key, columnName,
					StringSerializer.get());

			updateSecondaryIndexColumnFamily(key, columnName, columnValue, timestamp, ttl,
					oldColumn != null ? oldColumn.getValue() : "");
		}

	}

	/**
	 * This will update the secondary index column family properly, also deleting the previous values if needed.
	 *
	 * @param key            : key from the original column family. This will be the column name of the secondary
	 *                       index column family.
	 * @param columnName     : name of the column to be indexed.
	 * @param columnValue    : value of the column to be indexed. This can be null if we are indexing based on the
	 *                       column names only.
	 * @param timestamp      : timestamp for the new entry in the secondary index.
	 * @param ttl            : time to live for the new entry.
	 * @param oldColumnValue : old column related to this update so that we can retrieve the old value of the column
	 *                       to
	 *                       delete the previous secondary index.
	 */
	private void updateSecondaryIndexColumnFamily(K key, String columnName, String columnValue, long timestamp, int ttl,
			String oldColumnValue) {
		final Mutator<String> secondaryIndexMutator = HFactory.createMutator(keysp, StringSerializer.get());
		String secondaryIndexCF = secondaryIndexedColumnFamilyTemplate.getColumnFamily();

		// enqueue a deletion to the secondary index to remove the previous value of this column.
		if (!StringUtils.isBlank(oldColumnValue) && !"null".equalsIgnoreCase(oldColumnValue)) {
			secondaryIndexMutator.addDeletion(getSecondaryIndexKey(columnName, oldColumnValue), secondaryIndexCF, key,
					keySerializer);
		}

		// add the new value to the secondary index CF. Make sure to handle the TTLs properly.
		if (ttl > 0) {
			secondaryIndexMutator.addInsertion(getSecondaryIndexKey(columnName, columnValue), secondaryIndexCF,
					HFactory.createColumn(key, key,
							timestamp > 0 ? timestamp : keysp.createClock(),
									ttl, keySerializer, keySerializer));
		} else {
			secondaryIndexMutator.addInsertion(getSecondaryIndexKey(columnName, columnValue), secondaryIndexCF,
					HFactory.createColumn(key, key,
							timestamp > 0 ? timestamp : keysp.createClock(),
									keySerializer, keySerializer));
		}


		// execute everything (the deletion and the insertion) together
		MutationResult execute = secondaryIndexMutator.execute();
		log.debug("Secondary Index column " + columnName + " got updated for the key " + key.toString() +
				". Exec Time = " +
				execute.getExecutionTimeMicro() + ", Host used = " + execute.getHostUsed());
	}

	public CassandraResultSet<K, String> retrieveBySecondaryIndex(String columnName, String columnValue) {
		// first some fact checking, before going to Cassandra
		if (isSecondaryIndexByColumnNameAndValueEnabled && Collections.binarySearch(columnsToIndexOnColumnNameAndValue,
				columnName) >= 0) {
			return retrieveFromSecodaryIndex(columnName, columnValue);
		}

		return null;
	}

	@Override
	public CassandraResultSet<K, String> retrieveBySecondaryIndex(String columnName, List<String> columnValues) {
		// first some fact checking, before going to Cassandra
		if (isSecondaryIndexByColumnNameAndValueEnabled && Collections.binarySearch(columnsToIndexOnColumnNameAndValue,
				columnName) >= 0) {
			return retrieveFromSecondaryIndex(columnName, columnValues);
		}
		return null;

	}

	private CassandraResultSet<K, String> retrieveFromSecondaryIndex(String columnName, List<String> columnValues) {
		Map<String, List<K>> keysMap = retrieveKeysFromSecondaryIndex(columnName, columnValues);

		if (MapUtils.isNotEmpty(keysMap)) {
			Set<K> keys = new HashSet<>();
			for (List<K> keysForColValue : keysMap.values()) {
				keys.addAll(keysForColValue);
			}
			return readAllColumns(keys);
		}
		return null;
	}

	/**
	 * Enables you to retrieve objects using the mappings found in secondary index table. Its a good idea to make
	 * sure the given column name and the value are part of a secondary index before doing this call.
	 *
	 * @param columnName
	 * @param columnValue
	 *
	 * @return
	 */
	private CassandraResultSet<K, String> retrieveFromSecodaryIndex(String columnName, String columnValue) {
		List<K> mappingObjectIds = retrieveKeysFromSecondaryIndex(columnName, columnValue);
		if (CollectionUtils.isNotEmpty(mappingObjectIds)) {
			return readAllColumns(new HashSet<K>(mappingObjectIds));
		}

		return null;
	}

	@Override
	public CassandraResultSet<K, String> retrieveByColumnNameBasedSecondaryIndex(String columnName) {
		if (StringUtils.isNotEmpty(columnName) && columnName.matches(secondaryIdxByColumnPattern)) {
			return retrieveFromSecodaryIndex(columnName, "");
		}

		return null;
	}


	/**
	 * @param parameters
	 * @param limit      defines the number of **COLUMNS** to return, not the number of rows.
	 *
	 * @return
	 */
	@Override
	public CassandraResultSet readAllColumnsBySecondaryIndex(Map<String, String> parameters, int limit) {
		IndexedSlicesQuery<String, String, String> indexedSlicesQuery = HFactory.createIndexedSlicesQuery(keysp,
				StringSerializer.get(), StringSerializer.get(), StringSerializer.get());

		for (Map.Entry<String, String> entry : parameters.entrySet()) {
			indexedSlicesQuery.addEqualsExpression(entry.getKey(), entry.getValue());
		}
		indexedSlicesQuery.setRange("", "", false, limit);

		indexedSlicesQuery.setColumnFamily(this.columnFamily);
		QueryResult<OrderedRows<String, String, String>> result = indexedSlicesQuery.execute();
		return new HectorRowSliceResultSet(result);
	}

	@Override
	public Long getCounterValue(K key, String counterColumnName) {
		CounterQuery<K, String> counter = new ThriftCounterColumnQuery<K, String>(keysp, keySerializer,
				StringSerializer.get());
		QueryResult<HCounterColumn<String>> columnQueryResult = counter.setColumnFamily(columnFamily).setKey(key)
				.setName(counterColumnName).execute();
		if (columnQueryResult != null && columnQueryResult.get() != null) {
			Long value = columnQueryResult.get().getValue();
			return value != null ? value : 0L;
		} else {
			return 0L;
		}
	}

	@Override
	public void updateCounter(K key, String counterColumnName, long value) {
		final Mutator<K> m = HFactory.createMutator(keysp, keySerializer);
		m.incrementCounter(key, getColumnFamilyName(), counterColumnName, value);
	}

	@Override
	public void incrementCounter(K key, String counterColumnName) {
		this.updateCounter(key, counterColumnName, 1);
	}

	@Override
	public void decrementCounter(K key, String counterColumnName) {
		this.updateCounter(key, counterColumnName, -1);
	}


	/**
	 * Retrieves all the columns related to a given key.
	 * Analogous to "Select * from TABLE" in SQL world.
	 *
	 * @param key - key of the column to be read.
	 *
	 * @return CassandraResultSet (interface to get column values)
	 */
	public CassandraResultSet<K, String> readAllColumns(K key) throws HectorException {
		try {
			if (maxColumnCount > 0) {
				return readColumnSlice(key, null, null, false, maxColumnCount);
			} else {
				ColumnFamilyResult<K, String> queriedColumns = getColumnFamily().queryColumns(key);
				if (isClientAdapterDebugMessagesEnabled) {
					log.info("Row retrieved from Cassandra. Exec Time (micro-sec) = " +
							queriedColumns.getExecutionTimeMicro() +
							", Host used = " + queriedColumns.getHostUsed() + ", Key = " + key);
				}
				return new HectorResultSet<K, String>(queriedColumns);
			}

		} catch (HectorException e) {
			log.debug("HecubaClientManager error while reading key " + key.toString());
			if (log.isDebugEnabled()) {
				log.debug("Caught Exception", e);

				// lets see whether we have any issues with the downed nodes.
				logDownedHosts();
			}
			throw e;
		}
	}

	private CassandraResultSet<String, K> readSiColumnSlice(String key, boolean reversed, int count) {
		SliceQuery<String, K, K> sliceQuery = HFactory.createSliceQuery(keysp, secondaryIndexedColumnFamilyTemplate
				.getKeySerializer(), keySerializer, keySerializer).setColumnFamily(
						secondaryIndexedColumnFamilyTemplate.getColumnFamily()).setRange(null, null, reversed, count).setKey(
								key);


		QueryResult<ColumnSlice<K, K>> queriedResult = sliceQuery.execute();

		if (isClientAdapterDebugMessagesEnabled) {
			log.info("ColumnSlice retrieved from Cassandra. Exec Time = " + queriedResult.getExecutionTimeMicro() +
					", Host used = " + queriedResult.getHostUsed() + ", Key = " + key);
		}

		return new HectorColumnSliceResultSet<String, K, K>(queriedResult);
	}

	private CassandraResultSet<String, K> readSiColumnSlice(List<String> keys, boolean reversed, int count) {
		MultigetSliceQuery<String, K, K> multiGetSliceResult = HFactory.createMultigetSliceQuery(keysp,
				secondaryIndexedColumnFamilyTemplate.getKeySerializer(), keySerializer, keySerializer).setColumnFamily(
						secondaryIndexedColumnFamilyTemplate.getColumnFamily()).setRange(
								null, null, reversed, count).setKeys(keys);


		QueryResult<Rows<String, K, K>> queryResult = multiGetSliceResult.execute();

		if (isClientAdapterDebugMessagesEnabled) {
			log.info("ColumnSlice retrieved from Cassandra. Exec Time = " + queryResult.getExecutionTimeMicro() +
					", Host used = " + queryResult.getHostUsed() + ", Keys = " + Joiner.on(",").join(keys));
		}

		return new HectorRowSliceResultSet(queryResult);
	}


	@Override
	public CassandraResultSet<K, String> readColumnSlice(K key, String start, String end, boolean reversed, int count) {
		SliceQuery<K, String, String> sliceQuery = HFactory.createSliceQuery(keysp, keySerializer,
				StringSerializer.get(),
				StringSerializer.get()).setColumnFamily(
						columnFamily).setRange(start, end, reversed, count).setKey(key);

		QueryResult<ColumnSlice<String, String>> queriedResult = sliceQuery.execute();

		if (isClientAdapterDebugMessagesEnabled) {
			log.info("ColumnSlice retrieved from Cassandra. Exec Time = " + queriedResult.getExecutionTimeMicro() +
					", Host used = " + queriedResult.getHostUsed() + ", Key = " + key);
		}

		return new HectorColumnSliceResultSet<K, String, String>(queriedResult);
	}


	public CassandraResultSet<K, String> readColumns(K key, List<String> columns) throws HectorException {
		ColumnFamilyResult<K, String> queriedColumns;
		try {
			queriedColumns = getColumnFamily().queryColumns(key, columns);
			if (isClientAdapterDebugMessagesEnabled) {
				log.info(columns.size() + " columns retrieved from Cassandra. Exec Time = " +
						queriedColumns.getExecutionTimeMicro() + ", Host used = " +
						queriedColumns.getHostUsed());
			}
			return new HectorResultSet<K, String>(queriedColumns);
		} catch (HectorException e) {
			log.info("HecubaClientManager error while retrieving " + columns.size() + " columns for key " +
					key.toString());
			if (log.isDebugEnabled()) {
				log.debug("Caught Exception", e);
			}
			throw e;
		}
	}

	/**
	 * Reads the value of a column related to a given key.
	 *
	 * @param key        - key of the column to be read.
	 * @param columnName - name of the column to be read.
	 *
	 * @return
	 */
	public String readString(K key, String columnName) {
		HColumn<String, String> result = readColumn(key, columnName);
		return result == null ? null : result.getValue();
	}

	@Override
	public CassandraColumn readColumnInfo(K key, String columnName) {
		HColumn<String, String> result = readColumn(key, columnName);
		return result == null ? null : new CassandraColumn(result.getName(), result.getValue(), result.getClock(),
				result.getTtl());
	}

	private HColumn<String, String> readColumn(K key, String columnName) {
		HColumn<String, String> result = null;
		try {
			result = getColumnFamily().querySingleColumn(key, columnName, StringSerializer.get());
		} catch (Exception e) {

		}
		return result;
	}

	@Override
	/**
	 * @param clusterName
	 *            the clusterName to set
	 */
	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
		reConfigureParameters();
	}

	public void setLocationURL(String locationURL) {
		this.locationURLs = locationURL;
		reConfigureParameters();
	}

	/**
	 * @param columnFamily the columnFamily to set
	 */
	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
		reConfigureParameters();
	}

	private void configureHectorSpecificProperties() {

		hectorClientConfiguration = new HectorClientConfiguration();
		final Configuration configuration = ConfigUtils.getInstance().getInstance().getConfiguration();
		hectorClientConfiguration.setLoadBalancingPolicy(configuration.getString(
				HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".hectorpools.loadbalancingpolicy",
				"DynamicLoadBalancingPolicy"));
		hectorClientConfiguration.setMaxActive(configuration.getInteger(
				HecubaConstants.GLOBAL_PROP_NAME_PREFIX + "hectorpools.maxactive", 50));
		hectorClientConfiguration.setMaxIdle(configuration.getInteger(
				HecubaConstants.GLOBAL_PROP_NAME_PREFIX + "hectorpools.maxidle", -1));

		hectorClientConfiguration.setRetryDownedHosts(configuration.getBoolean(
				HecubaConstants.GLOBAL_PROP_NAME_PREFIX + "hectorpools.retrydownedhosts", true));

		hectorClientConfiguration.setRetryDownedHostsDelayInSeconds(configuration.getInteger(
				HecubaConstants.GLOBAL_PROP_NAME_PREFIX + "hectorpools.retrydownedhostsinseconds", 30));

		hectorClientConfiguration.setThriftSocketTimeout(configuration.getInteger(
				HecubaConstants.GLOBAL_PROP_NAME_PREFIX + "hectorpools.thriftsockettimeout", 100));

		hectorClientConfiguration.setUseThriftFramedTransport(configuration.getBoolean(
				HecubaConstants.GLOBAL_PROP_NAME_PREFIX + "hectorpools.usethriftframedtransport", true));
	}

	private void reConfigureParameters() {

		columnFamilyTemplates = new ArrayList<ColumnFamilyTemplate<K, String>>();
		configureHectorPools();
	}

	private void configureHectorPools() {

		final String listOfNodesAndPorts = getListOfNodesAndPorts(locationURLs, ports);

		final CassandraHostConfigurator cassandraHostConfigurator = createCassandraConfigurator();
		cassandraHostConfigurator.setHosts(listOfNodesAndPorts);
		log.info("Hector pool created for " + listOfNodesAndPorts);

		cluster = HFactory.getOrCreateCluster(clusterName, cassandraHostConfigurator);

		keysp = HFactory.createKeyspace(keyspace, cluster, consistencyLevel);
		columnFamilyTemplates.add(new ThriftColumnFamilyTemplate<K, String>(keysp, columnFamily, keySerializer,
				StringSerializer.get()));

		// now, if we have secondary indexed columns, then go ahead and create its own column family template.
		if (isSecondaryIndexesByColumnNamesEnabled ||
				(columnsToIndexOnColumnNameAndValue != null && columnsToIndexOnColumnNameAndValue.size() > 0)) {
			String secondaryIndexedColumnFamily = ConfigUtils.getInstance().getConfiguration().getString(
					HecubaConstants.GLOBAL_PROP_NAME_PREFIX + "." + columnFamily + ".secondaryIndexCF",
					columnFamily + HecubaConstants.SECONDARY_INDEX_CF_NAME_SUFFIX);
			secondaryIndexedColumnFamilyTemplate = new ThriftColumnFamilyTemplate<String, K>(keysp,
					secondaryIndexedColumnFamily,
					StringSerializer.get(),
					keySerializer);
		}

	}


	private CassandraHostConfigurator createCassandraConfigurator() {

		final CassandraHostConfigurator cassandraHostConfigurator = new CassandraHostConfigurator();
		cassandraHostConfigurator.setMaxActive(hectorClientConfiguration.getMaxActive());
		// cassandraHostConfigurator.setMaxIdle(hectorClientConfiguration.getMaxIdle());
		cassandraHostConfigurator.setCassandraThriftSocketTimeout(hectorClientConfiguration.getThriftSocketTimeout());
		cassandraHostConfigurator.setRetryDownedHosts(hectorClientConfiguration.isRetryDownedHosts());
		cassandraHostConfigurator.setRetryDownedHostsDelayInSeconds(
				hectorClientConfiguration.getRetryDownedHostsDelayInSeconds());
		cassandraHostConfigurator.setLoadBalancingPolicy(hectorClientConfiguration.getLoadBalancingPolicy());
		log.info("Hector Host Configurator Parameters \n" + cassandraHostConfigurator.toString());
		return cassandraHostConfigurator;
	}

	public void configureOurColumnFamilyTemplatePool(String[] splittedPorts, String[] splittedLocationURLs) {
		final CassandraHostConfigurator cassandraHostConfigurator = createCassandraConfigurator();
		for (int index = 0; index < splittedLocationURLs.length; index++) {
			String locationURL = splittedLocationURLs[index];
			String port = "";
			if (index < splittedPorts.length) {
				port = splittedPorts[index];
			}

			if (port == null || "".equals(port)) {
				port = "9160";
			}

			cassandraHostConfigurator.setHosts(locationURL + ":" + port);
			cluster = HFactory.getOrCreateCluster(clusterName, cassandraHostConfigurator);
			keysp = HFactory.createKeyspace(keyspace, cluster, consistencyLevel);
			columnFamilyTemplates.add(new ThriftColumnFamilyTemplate<K, String>(keysp, columnFamily, keySerializer,
					StringSerializer.get()));
		}
	}

	public Cluster getCluster() {
		return cluster;
	}

	public List<ColumnFamilyTemplate<K, String>> getColumnFamilyTemplates() {
		return columnFamilyTemplates;
	}

	ColumnFamilyTemplate<K, String> getColumnFamily() {
		return columnFamilyTemplates.get(0);
	}

	@Override
	public void createKeyspaceAndColumnFamilies(String keyspace, List<ColumnFamilyInfo> columnFamilies) {
		createKeyspace(keyspace);
		for (ColumnFamilyInfo columnFamilyInfo : columnFamilies) {
			addColumnFamily(keyspace, columnFamilyInfo.getName());
		}

	}

	/**
	 * Retrieves all the columns for the list of keys. <p/>
	 *
	 * @param keys - set of keys to retrieve
	 */
	@Override
	public CassandraResultSet<K, String> readAllColumns(Set<K> keys) throws HectorException {
		try {
			if (maxColumnCount > 0) {
				return readColumnSlice(keys, null, null, false, maxColumnCount);
			} else {
				ColumnFamilyResult<K, String> queriedColumns = getColumnFamily().queryColumns(keys);
				if (isClientAdapterDebugMessagesEnabled) {
					log.info("Rows retrieved from Cassandra [Hector] (for " + keys.size() + " keys). Exec Time " +
							"(micro-sec) = " + queriedColumns.getExecutionTimeMicro() + ", Host used = " +
							queriedColumns.getHostUsed());
				}
				return new HectorResultSet<K, String>(queriedColumns);
			}
		} catch (HectorException e) {
			log.warn("HecubaClientManager [Hector] error while reading multiple keys. Number of keys = " +
					keys.size() + ", keys = " + keys.toString());
			if (log.isDebugEnabled()) {
				log.debug("Caught Exception while reading for multiple keys", e);
			}
			throw e;
		}
	}

	@Override
	public CassandraResultSet<K, String> readColumnSlice(Set<K> keys, String start, String end, boolean reversed,
			int count) {
		MultigetSliceQuery<K, String, String> multigetSliceQuery = HFactory.createMultigetSliceQuery(keysp,
				keySerializer, StringSerializer.get(), StringSerializer.get()).setColumnFamily(columnFamily).setRange(
						start, end, reversed, count).setKeys(keys);

		QueryResult<Rows<K, String, String>> queriedResult = multigetSliceQuery.execute();

		if (isClientAdapterDebugMessagesEnabled) {
			log.info("ColumnSlice retrieved from Cassandra. Exec Time = " + queriedResult.getExecutionTimeMicro() +
					", Host used = " + queriedResult.getHostUsed() + ", No. of Keys = " + keys.size());
		}

		return new HectorRowSliceResultSet<K, String, String>(queriedResult);
	}

	@Override
	public CassandraResultSet<K, String> readColumns(Set<K> keys, List<String> columnNames) throws Exception {
		if (CollectionUtils.isNotEmpty(columnNames)) {
			ColumnFamilyResult<K, String> queriedColumns;
			try {
				queriedColumns = getColumnFamily().queryColumns(keys, columnNames, null);
				if (isClientAdapterDebugMessagesEnabled) {
					log.info(columnNames.size() + " columns retrieved from Cassandra [Hector] for " + keys.size() +
							" keys . Exec Time (micro-sec) = " + queriedColumns.getExecutionTimeMicro() +
							", Host used = " + queriedColumns.getHostUsed());
				}
				return new HectorResultSet<K, String>(queriedColumns);
			} catch (HectorException e) {
				log.info("HecubaClientManager error while retrieving " + columnNames.size() + " columns. keys = " +
						keys.toString());
				if (log.isDebugEnabled()) {
					log.debug("Caught Exception", e);
				}
				throw e;
			}
		} else {
			return readAllColumns(keys);
		}
	}

	@Override
	public void deleteColumns(K key, List<String> columnNameList) {
		for (String columnName : columnNameList) {
			deleteColumn(key, columnName);
		}
	}

	@Override
	public List<K> retrieveKeysBySecondaryIndex(String columnName, String columnValue) {
		// first some fact checking, before going to Cassandra
		if (isSecondaryIndexByColumnNameAndValueEnabled && Collections.binarySearch(columnsToIndexOnColumnNameAndValue,
				columnName) >= 0) {
			return retrieveKeysFromSecondaryIndex(columnName, columnValue);
		}
		return null;
	}

	@Override
	public Map<String, List<K>> retrieveKeysBySecondaryIndex(String columnName,
			List<String> columnValues) {
		// first some fact checking, before going to Cassandra
		if (isSecondaryIndexByColumnNameAndValueEnabled && Collections.binarySearch(columnsToIndexOnColumnNameAndValue,
				columnName) >= 0) {
			return retrieveKeysFromSecondaryIndex(columnName, columnValues);
		}
		return null;
	}

	@Override
	public List<K> retrieveKeysByColumnNameBasedSecondaryIndex(String columnName) {
		if (StringUtils.isNotEmpty(columnName) && columnName.matches(secondaryIdxByColumnPattern)) {
			return retrieveKeysFromSecondaryIndex(columnName, "");
		}
		return null;
	}

	private List<K> retrieveKeysFromSecondaryIndex(String columnName, String columnValue) {
		String secondaryIndexKey = getSecondaryIndexKey(columnName, columnValue);
		CassandraResultSet<String, K> columns;
		try {
			if (maxSiColumnCount > 0) {
				columns = readSiColumnSlice(secondaryIndexKey, false, maxSiColumnCount);
			} else {
				columns = new HectorResultSet<String, K>(secondaryIndexedColumnFamilyTemplate.queryColumns(
						secondaryIndexKey));
			}

			if (isClientAdapterDebugMessagesEnabled) {
				log.debug("Secondary Index Row for " + secondaryIndexKey + " retrieved from Cassandra. Exec Time = " +
						columns.getExecutionLatency() + ", Host used = " + columns.getHost());
			}

			if (columns != null && CollectionUtils.isNotEmpty(columns.getColumnNames())) {
				return new ArrayList<>(columns.getColumnNames());
			}
		} catch (HectorException e) {
			log.debug("HecubaClientManager error while retrieving secondary index " + getSecondaryIndexKey(columnName,
					columnValue));
			if (log.isDebugEnabled()) {
				log.debug("Caught Exception", e);

				// lets see whether we have any issues with the downed nodes.
				logDownedHosts();
			}
			throw e;
		}
		return null;
	}

	private Map<String, List<K>> retrieveKeysFromSecondaryIndex(String columnName, List<String> columnValues) {
		List<String> secondaryIndexKeys = getSecondaryIndexKeys(columnName, columnValues);

		CassandraResultSet<String, K> resultSet;
		try {
			if (maxSiColumnCount > 0) {
				resultSet = readSiColumnSlice(secondaryIndexKeys, false, maxSiColumnCount);
			} else {
				resultSet = new HectorResultSet<>(secondaryIndexedColumnFamilyTemplate.queryColumns(secondaryIndexKeys));
			}

			if (isClientAdapterDebugMessagesEnabled) {
				log.debug("Secondary Index Rows for " + Joiner.on(",").join(columnValues) +
						" retrieved from Cassandra. Exec Time = " +
						resultSet.getExecutionLatency() + ", Host used = " + resultSet.getHost());
			}

			if (resultSet != null) {
				Map<String, List<K>> keysMap = new HashMap<>();
				//Deals with the case where the first element is a miss.
				if (CollectionUtils.isNotEmpty(resultSet.getColumnNames())) {
					keysMap.put(getSecondaryIndexedColumnValue(resultSet.getKey()), new ArrayList<>(resultSet.getColumnNames()));
				}

				while (resultSet.hasNextResult()) {
					resultSet.nextResult();
					if (CollectionUtils.isNotEmpty(resultSet.getColumnNames())) {
						keysMap.put(getSecondaryIndexedColumnValue(resultSet.getKey()), new ArrayList<>(resultSet.getColumnNames()));
					}
				}
				return keysMap;
			}
		} catch (HectorException e) {
			log.debug("HecubaClientManager error while retrieving secondary index for multiple siKeys, " + Joiner.on(",").join(
					secondaryIndexKeys));
			if (log.isDebugEnabled()) {
				log.debug("Caught Exception", e);

				// lets see whether we have any issues with the downed nodes.
				logDownedHosts();
			}
			throw e;
		}
		return null;
	}

	@Override
	protected void logDownedHosts() {
		HConnectionManager connectionManager = this.cluster.getConnectionManager();
		StringBuilder allHostsSB = new StringBuilder("All Hosts = {");
		for (CassandraHost host : connectionManager.getHosts()) {
			allHostsSB.append(host.getHost() + ", ");
		}
		if (allHostsSB.toString().endsWith(", ")) {
			allHostsSB.setLength(allHostsSB.length() - 2);
		}
		allHostsSB.append("}");
		log.debug(allHostsSB.toString());
		
		StringBuilder downedHostsSB = new StringBuilder("Downed Hosts = {");
		for (CassandraHost host : connectionManager.getDownedHosts()) {
			downedHostsSB.append(host.getHost() + ", ");
		}
		if (downedHostsSB.toString().endsWith(", ")) {
			downedHostsSB.setLength(downedHostsSB.length() - 2);
		}
		downedHostsSB.append("}");
		log.debug(downedHostsSB.toString());
	}
}
