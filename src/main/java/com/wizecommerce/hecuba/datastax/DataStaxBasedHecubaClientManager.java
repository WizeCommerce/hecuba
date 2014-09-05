package com.wizecommerce.hecuba.datastax;

import java.nio.ByteBuffer;
import java.util.*;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.math.NumberUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.policies.*;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.wizecommerce.hecuba.*;
import com.wizecommerce.hecuba.util.ClientManagerUtils;
import com.wizecommerce.hecuba.util.ConfigUtils;

public class DataStaxBasedHecubaClientManager<K> extends HecubaClientManager<K> {
	private static final Logger logger = LoggerFactory.getLogger(DataStaxBasedHecubaClientManager.class);
	private static final Configuration configuration = ConfigUtils.getInstance().getConfiguration();
	private static final int statementCacheMaxSize = configuration.getInt(HecubaConstants.DATASTAX_STATEMENT_CACHE_MAX_SIZE, 1000);
	private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.hourMinuteSecondMillis();

	private DataType keyType;

	private String datacenter;
	private String[] endpoints;
	private int port;
	private String keyspace;

	private String columnFamily;
	private String secondaryIndexColumnFamily;

	private String username;
	private String password;

	private ConsistencyLevel readConsistencyLevel;
	private ConsistencyLevel writeConsistencyLevel;

	private int connectTimeout;
	private int readTimeout;
	private int maxConnectionsPerHost;
	private int statementFetchSize;

	private boolean compressionEnabled;
	private boolean tracingEnabled;

	private Session session;

	private String keyColumn;
	private String secondaryIndexKeyColumn;

	private LoadingCache<String, PreparedStatement> readStatementCache = CacheBuilder.newBuilder().maximumSize(statementCacheMaxSize)
			.build(new CacheLoader<String, PreparedStatement>() {
				@Override
				public PreparedStatement load(String query) throws Exception {
					PreparedStatement stmt = session.prepare(query);
					stmt.setConsistencyLevel(readConsistencyLevel);
					if (tracingEnabled) {
						stmt.enableTracing();
					}
					return stmt;
				}
			});
	private LoadingCache<String, PreparedStatement> writeStatementCache = CacheBuilder.newBuilder().maximumSize(statementCacheMaxSize)
			.build(new CacheLoader<String, PreparedStatement>() {
				@Override
				public PreparedStatement load(String query) throws Exception {
					PreparedStatement stmt = session.prepare(query);
					stmt.setConsistencyLevel(writeConsistencyLevel);
					if (tracingEnabled) {
						stmt.enableTracing();
					}
					return stmt;
				}
			});
	private Cluster cluster;

	public DataStaxBasedHecubaClientManager(CassandraParamsBean parameters, DataType keyType) {
		super(parameters);

		Configuration configuration = ConfigUtils.getInstance().getConfiguration();

		this.keyType = keyType;

		datacenter = parameters.getDataCenter();
		if (StringUtils.isEmpty(this.datacenter)) {
			datacenter = configuration.getString(HecubaConstants.DATASTAX_DATACENTER, null);

			if (StringUtils.isEmpty(this.datacenter)) {
				logger.warn("Datacenter is unset.  It is recommended to be set or performance may degrade unexpectedly.");
			}
		}

		endpoints = Iterables.toArray(Splitter.on(":").split(parameters.getLocationURLs()), String.class);
		keyspace = '"' + parameters.getKeyspace() + '"';
		columnFamily = '"' + parameters.getColumnFamily() + '"';
		secondaryIndexColumnFamily = '"' + getSecondaryIndexColumnFamily(parameters) + '"';
		port = NumberUtils.toInt(parameters.getCqlPort());
		username = parameters.getUsername();
		password = parameters.getPassword();

		readConsistencyLevel = getConsistencyLevel(parameters, "read");
		writeConsistencyLevel = getConsistencyLevel(parameters, "write");

		connectTimeout = configuration.getInt(HecubaConstants.DATASTAX_CONNECT_TIMEOUT, connectTimeout);
		readTimeout = configuration.getInt(HecubaConstants.DATASTAX_READ_TIMEOUT, readTimeout);
		maxConnectionsPerHost = configuration.getInt(HecubaConstants.DATASTAX_MAX_CONNECTIONS_PER_HOST, maxConnectionsPerHost);
		compressionEnabled = configuration.getBoolean(HecubaConstants.DATASTAX_COMPRESSION_ENABLED, compressionEnabled);
		tracingEnabled = configuration.getBoolean(HecubaConstants.DATASTAX_TRACING_ENABLED, tracingEnabled);
		statementFetchSize = configuration.getInteger(HecubaConstants.DATASTAX_STATEMENT_FETCH_SIZE, statementFetchSize);

		init();

		keyColumn = getKeyColumn(columnFamily);

		secondaryIndexKeyColumn = getKeyColumn(secondaryIndexColumnFamily);

		logger.info("{}", ToStringBuilder.reflectionToString(this));
	}

	private String getKeyColumn(String columnFamily) {
		KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace);
		TableMetadata tableMetadata = keyspaceMetadata.getTable(columnFamily);

		if (tableMetadata == null) {
			return null;
		}

		for (String key : new String[] { "\"KEY\"", "key" }) {
			if (tableMetadata.getColumn(key) != null) {
				return key;
			}
		}

		return null;
	}

	private ConsistencyLevel getConsistencyLevel(CassandraParamsBean parameters, String operation) {
		Configuration configuration = ConfigUtils.getInstance().getConfiguration();

		String consistencyLevel = "ONE";
		String[] consistencyPolicyProperties = HecubaConstants.getConsistencyPolicyProperties(parameters.getColumnFamily(), operation);

		for (String property : consistencyPolicyProperties) {
			consistencyLevel = configuration.getString(property, consistencyLevel);
		}

		return ConsistencyLevel.valueOf(consistencyLevel);
	}

	private String getSecondaryIndexColumnFamily(CassandraParamsBean parameters) {
		Configuration configuration = ConfigUtils.getInstance().getConfiguration();
		String secondaryIndexColumnFamilyProperty = HecubaConstants.getSecondaryIndexColumnFamilyProperty(parameters.getColumnFamily());
		String defaultSecondaryIndexColumnFamily = parameters.getColumnFamily() + HecubaConstants.SECONDARY_INDEX_CF_NAME_SUFFIX;
		return configuration.getString(secondaryIndexColumnFamilyProperty, defaultSecondaryIndexColumnFamily);
	}

	@Override
	public void deleteColumn(K key, String columnName) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		builder.append("BEGIN UNLOGGED BATCH\n");

		if (isSecondaryIndexByColumnNameEnabledForColumn(columnName)) {
			builder.append("\tDELETE FROM " + secondaryIndexColumnFamily + " where " + secondaryIndexKeyColumn + " = ? and column1 = ?;\n");
			values.add(getSecondaryIndexKey(columnName, ""));
			values.add(convertKey(key));
		}

		if (isSecondaryIndexByColumnNameAndValueEnabledForColumn(columnName)) {
			String oldValue = readString(key, columnName);
			builder.append("\tDELETE FROM " + secondaryIndexColumnFamily + " where " + secondaryIndexKeyColumn + " = ? and column1 = ?;\n");
			values.add(getSecondaryIndexKey(columnName, oldValue));
			values.add(convertKey(key));
		}

		builder.append("\tDELETE FROM " + columnFamily + " WHERE " + keyColumn + " = ? and column1 = ?;\n");
		values.add(convertKey(key));
		values.add(columnName);

		builder.append("APPLY BATCH;");

		write(builder.toString(), values.toArray());
	}

	@Override
	public void deleteColumns(K key, List<String> columnNames) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		builder.append("BEGIN UNLOGGED BATCH\n");

		try {
			if (isSecondaryIndexByColumnNameAndValueEnabled || isSecondaryIndexesByColumnNamesEnabled) {
				// Find obsolete secondary indexes
				CassandraResultSet<K, String> oldValues = readColumns(key, columnNames);
				List<String> secondaryIndexesToDelete = new ArrayList<>();
				for (String columnName : columnNames) {
					if (isSecondaryIndexByColumnNameEnabledForColumn(columnName)) {
						secondaryIndexesToDelete.add(getSecondaryIndexKey(columnName, ""));
					}
					if (isSecondaryIndexByColumnNameAndValueEnabledForColumn(columnName)) {
						secondaryIndexesToDelete.add(getSecondaryIndexKey(columnName, oldValues.getString(columnName)));
					}
				}

				// Delete obsolete secondary indexes
				if (secondaryIndexesToDelete.size() > 0) {
					for (String secondaryIndexKey : secondaryIndexesToDelete) {
						builder.append("\tDELETE FROM " + secondaryIndexColumnFamily + " WHERE " + secondaryIndexKeyColumn + " = ? and column1 = ?;\n");
						values.add(secondaryIndexKey);
						values.add(convertKey(key));
					}
				}
			}

		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		for (String columnName : columnNames) {
			builder.append("\tDELETE FROM " + columnFamily + " where " + keyColumn + " = ? and column1 = ?;\n");
			values.add(convertKey(key));
			values.add(columnName);
		}

		builder.append("APPLY BATCH;");

		write(builder.toString(), values.toArray());
	}

	@Override
	public void deleteRow(K key, long timestamp) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		builder.append("BEGIN UNLOGGED BATCH\n");

		if (isSecondaryIndexByColumnNameAndValueEnabled || isSecondaryIndexesByColumnNamesEnabled) {
			try {
				// Find obsolete secondary indexes
				List<String> secondaryIndexesToDelete = new ArrayList<>();
				final CassandraResultSet<K, String> oldValues = readAllColumns(key);
				if (oldValues.hasResults()) {
					for (String columnName : oldValues.getColumnNames()) {
						if (isSecondaryIndexByColumnNameEnabledForColumn(columnName)) {
							secondaryIndexesToDelete.add(getSecondaryIndexKey(columnName, ""));
						}
						if (isSecondaryIndexByColumnNameAndValueEnabledForColumn(columnName)) {
							secondaryIndexesToDelete.add(getSecondaryIndexKey(columnName, oldValues.getString(columnName)));
						}
					}
				}

				// Delete obsolete secondary indexes
				if (secondaryIndexesToDelete.size() > 0) {
					for (String secondaryIndexKey : secondaryIndexesToDelete) {
						builder.append("\tDELETE FROM " + secondaryIndexColumnFamily);

						if (timestamp > 0) {
							builder.append(" USING TIMESTAMP ?");
							values.add(timestamp);
						}

						builder.append(" WHERE " + secondaryIndexKeyColumn + " = ? and column1 = ?;\n");
						values.add(secondaryIndexKey);
						values.add(convertKey(key));
					}
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		builder.append("\tDELETE FROM " + columnFamily);

		if (timestamp > 0) {
			builder.append(" USING TIMESTAMP ?");
			values.add(timestamp);
		}

		builder.append(" WHERE " + keyColumn + " = ?;\n");
		values.add(convertKey(key));

		builder.append("APPLY BATCH;");

		write(builder.toString(), values.toArray());
	}

	@Override
	public Long getCounterValue(K key, String counterColumnName) {
		final String query = "select * from " + columnFamily + " where " + keyColumn + " = ? and column1 = ?";

		CassandraResultSet<K, String> result = read(query, null, null, ImmutableMap.of(counterColumnName, DataType.counter()), convertKey(key), counterColumnName);

		if (result.hasResults()) {
			return result.getLong(counterColumnName);
		}

		return null;
	}

	@Override
	public void updateCounter(K key, String counterColumnName, long value) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		builder.append("UPDATE " + columnFamily + " set value = value + ? where " + keyColumn + " = ? and column1 = ?");
		values.add(value);
		values.add(convertKey(key));
		values.add(counterColumnName);

		write(builder.toString(), values.toArray());
	}

	@Override
	public void incrementCounter(K key, String counterColumnName) {
		updateCounter(key, counterColumnName, 1);
	}

	@Override
	public void decrementCounter(K key, String counterColumnName) {
		updateCounter(key, counterColumnName, -1);
	}

	@Override
	public CassandraResultSet<K, String> readAllColumns(K key) throws Exception {
		final String query = "select * from " + columnFamily + " where " + keyColumn + " = ?";

		return read(query, convertKey(key));
	}

	@Override
	public CassandraResultSet<K, String> readAllColumns(Set<K> keys) throws Exception {
		final String query = "select * from " + columnFamily + " where " + keyColumn + " in ?";

		return read(query, convertKeys(keys));

	}

	@Override
	@SuppressWarnings("rawtypes")
	public CassandraResultSet readAllColumnsBySecondaryIndex(Map<String, String> parameters, int limit) {
		List<Object> values = new ArrayList<>();
		StringBuilder builder = new StringBuilder();
		builder.append("select * from " + columnFamily + " where " + StringUtils.repeat("?=?", " and ", parameters.size()));

		if (limit > 0) {
			builder.append(" limit ?");
			values.add(limit);
		}

		for (Map.Entry<String, String> entry : parameters.entrySet()) {
			String column = entry.getKey();
			String value = entry.getValue();
			values.add(column);
			values.add(value);
		}

		return read(builder.toString(), values.toArray());
	}

	@Override
	public CassandraColumn readColumnInfo(K key, String columnName) {
		final String query = "select " + keyColumn + ", column1, value, writetime(value), ttl(value) from " + columnFamily + " where " + keyColumn + " = ? and column1 = ?";

		PreparedStatement stmt = readStatementCache.getUnchecked(query);

		BoundStatement bind = stmt.bind(convertKey(key), columnName);
		ResultSet rs = session.execute(bind);

		Iterator<Row> iterator = rs.iterator();
		if (iterator.hasNext()) {
			Row row = iterator.next();
			long timestamp = row.getLong("writetime(value)");
			int ttl = row.getInt("ttl(value)");
			String value = row.getString("value");
			return new CassandraColumn(columnName, value, timestamp, ttl);
		}

		return null;
	}

	@Override
	public CassandraResultSet<K, String> readColumns(K key, List<String> columnNames) throws Exception {
		if (CollectionUtils.isEmpty(columnNames)) {
			return readAllColumns(key);
		}

		final String query = "select * from " + columnFamily + " where " + keyColumn + " = ? and column1 in ?";

		CassandraResultSet<K, String> result = read(query, convertKey(key), columnNames);

		return result;
	}

	@Override
	public CassandraResultSet<K, String> readColumns(Set<K> keys, List<String> columnNames) throws Exception {
		if (CollectionUtils.isEmpty(columnNames)) {
			return readAllColumns(keys);
		}

		final String query = "select * from " + columnFamily + " where " + keyColumn + " in ? and column1 in ?";

		CassandraResultSet<K, String> result = read(query, convertKeys(keys), columnNames);

		return result;
	}

	@Override
	public CassandraResultSet<K, String> readColumnSlice(K key, String start, String end, boolean reversed, int count) {
		List<Object> values = new ArrayList<>();
		StringBuilder builder = new StringBuilder();
		builder.append("select * from " + columnFamily + " where " + keyColumn + " = ?");
		values.add(convertKey(key));

		if (start != null) {
			if (reversed) {
				builder.append(" and column1 <= ?");
			} else {
				builder.append(" and column1 >= ?");
			}
			values.add(start);
		}

		if (end != null) {
			if (reversed) {
				builder.append(" and column1 >= ?");
			} else {
				builder.append(" and column1 <= ?");
			}
			values.add(end);
		}

		if (reversed) {
			builder.append(" order by column1 desc");
		}

		if (count > 0) {
			builder.append(" limit ?");
			values.add(count);
		}

		return read(builder.toString(), values.toArray());
	}

	@Override
	public CassandraResultSet<K, String> readColumnSlice(Set<K> keys, String start, String end, boolean reversed) {
		// CQL3 does not support column slice count limit anymore (they used to have FIRST N), so removing support for "count"

		List<Object> values = new ArrayList<>();
		StringBuilder builder = new StringBuilder();
		builder.append("select * from " + columnFamily + " where " + keyColumn + " in ?");
		values.add(convertKeys(keys));

		if (start != null) {
			if (reversed) {
				builder.append(" and column1 <= ?");
			} else {
				builder.append(" and column1 >= ?");
			}
			values.add(start);
		}

		if (end != null) {
			if (reversed) {
				builder.append(" and column1 >= ?");
			} else {
				builder.append(" and column1 <= ?");
			}
			values.add(end);
		}

		// If reversed we'll just reverse internally, since Cassandra doesn't preserve key order
		CassandraResultSet<K, String> resultSet = read(builder.toString(), values.toArray());
		if (reversed) {
			resultSet = new ReversedColumnsCassandraResultSet<>(resultSet);
		}

		return resultSet;
	}

	@Override
	public String readString(K key, String columnName) {
		final String query = "select * from " + columnFamily + " where " + keyColumn + " = ? and column1 = ?";

		CassandraResultSet<K, String> result = read(query, convertKey(key), columnName);

		if (result.hasResults()) {
			return result.getString(columnName);
		}

		return null;
	}

	@Override
	public CassandraResultSet<K, String> retrieveByColumnNameBasedSecondaryIndex(String columnName) {
		List<K> keys = retrieveKeysByColumnNameBasedSecondaryIndex(columnName);
		if (keys != null) {
			try {
				CassandraResultSet<K, String> allColumns = readAllColumns(new HashSet<>(keys));
				if (allColumns.hasResults()) {
					return allColumns;
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	@Override
	public CassandraResultSet<K, String> retrieveBySecondaryIndex(String columnName, List<String> columnValue) {
		Map<String, List<K>> columnValueToKeysMap = retrieveKeysBySecondaryIndex(columnName, columnValue);
		if (MapUtils.isNotEmpty(columnValueToKeysMap)) {
			Set<K> keys = new HashSet<>();
			for (List<K> subKeys : columnValueToKeysMap.values()) {
				keys.addAll(subKeys);
			}

			try {
				CassandraResultSet<K, String> allColumns = readAllColumns(keys);
				if (allColumns.hasResults()) {
					return allColumns;
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return null;
	}

	@Override
	public CassandraResultSet<K, String> retrieveBySecondaryIndex(String columnName, String columnValue) {
		List<K> keys = retrieveKeysBySecondaryIndex(columnName, columnValue);

		if (CollectionUtils.isNotEmpty(keys)) {
			try {
				CassandraResultSet<K, String> allColumns = readAllColumns(new HashSet<>(keys));
				if (allColumns.hasResults()) {
					return allColumns;
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		return null;
	}

	@Override
	public List<K> retrieveKeysByColumnNameBasedSecondaryIndex(String columnName) {
		return retrieveKeysBySecondaryIndex(columnName, (String) null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Map<String, List<K>> retrieveKeysBySecondaryIndex(String columnName, List<String> columnValues) {
		final String query = "select * from " + secondaryIndexColumnFamily + " where " + secondaryIndexKeyColumn + " in ?";

		Map<String, String> secondaryIndexKeys = new HashMap<>();
		for (String columnValue : columnValues) {
			secondaryIndexKeys.put(getSecondaryIndexKey(columnName, columnValue), columnValue);
		}

		Map<String, List<K>> mapToKeys = new HashMap<>();
		CassandraResultSet<K, String> keysResultSet = read(query, DataType.ascii(), keyType, ImmutableMap.of("*", keyType), new ArrayList<>(secondaryIndexKeys.keySet()));
		while (keysResultSet.hasResults()) {
			List<K> keys = new ArrayList<>();
			for (String key : keysResultSet.getColumnNames()) {
				if (keyType == DataType.bigint()) {
					keys.add((K) NumberUtils.createLong(key));
				} else {
					keys.add((K) key);
				}
			}

			if (keys.size() > 0) {
				String secondaryIndexKey = (String) keysResultSet.getKey();
				String columnValue = secondaryIndexKeys.get(secondaryIndexKey);
				mapToKeys.put(columnValue, keys);
			}

			if (!keysResultSet.hasNextResult()) {
				break;
			}

			keysResultSet.nextResult();
		}

		return mapToKeys;
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<K> retrieveKeysBySecondaryIndex(String columnName, String columnValue) {
		final String query = "select * from " + secondaryIndexColumnFamily + " where " + secondaryIndexKeyColumn + " = ?";
		CassandraResultSet<K, String> keysResultSet = read(query, DataType.ascii(), keyType, ImmutableMap.of("*", keyType), getSecondaryIndexKey(columnName, columnValue));
		List<K> keys = new ArrayList<>();
		if (keysResultSet.hasResults()) {
			for (String key : keysResultSet.getColumnNames()) {
				if (keyType == DataType.bigint()) {
					keys.add((K) NumberUtils.createLong(key));
				} else {
					keys.add((K) key);
				}
			}

			return keys;
		}

		return null;
	}

	@Override
	public void updateByteBuffer(K key, String columnName, ByteBuffer value) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		builder.append("INSERT INTO " + columnFamily + " (" + keyColumn + ", column1, value) values (?,?,?)");
		values.add(convertKey(key));
		values.add(columnName);
		values.add(value);

		write(builder.toString(), values.toArray());
	}

	@Override
	public void updateRow(K key, Map<String, Object> row, Map<String, Long> timestamps, Map<String, Integer> ttls) throws Exception {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		updateSecondaryIndexes(key, row, timestamps, ttls);

		builder.append("BEGIN UNLOGGED BATCH\n");

		for (Map.Entry<String, Object> entry : row.entrySet()) {
			builder.append("\tINSERT INTO " + columnFamily + " (" + keyColumn + ", column1, value) values (?,?,?)");
			values.add(convertKey(key));
			values.add(entry.getKey());
			String valueToInsert = ClientManagerUtils.getInstance().convertValueForStorage(entry.getValue());
			values.add(valueToInsert);

			Long timestamp = timestamps != null ? timestamps.get(entry.getKey()) : null;
			Integer ttl = ttls != null ? ttls.get(entry.getKey()) : null;

			if (timestamp != null && timestamp > 0 && ttl != null && ttl > 0) {
				builder.append(" USING TIMESTAMP ? and TTL ?");
				values.add(timestamp);
				values.add(ttl);
			} else if (timestamp != null && timestamp > 0) {
				builder.append(" USING TIMESTAMP ?");
				values.add(timestamp);
			} else if (ttl != null && ttl > 0) {
				builder.append(" USING TTL ?");
				values.add(ttl);
			}

			builder.append(";\n");
		}

		builder.append("APPLY BATCH;");

		write(builder.toString(), values.toArray());
	}

	@Override
	public void updateString(K key, String columnName, String value, long timestamp, int ttl) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		updateSecondaryIndexes(key, columnName, value, timestamp, ttl);

		builder.append("INSERT INTO " + columnFamily + " (" + keyColumn + ", column1, value) values (?,?,?)");
		values.add(convertKey(key));
		values.add(columnName);
		values.add(value);

		if (timestamp > 0 && ttl > 0) {
			builder.append(" USING TIMESTAMP ? and TTL ?");
			values.add(timestamp);
			values.add(ttl);
		} else if (timestamp > 0) {
			builder.append(" USING TIMESTAMP ?");
			values.add(timestamp);
		} else if (ttl > 0) {
			builder.append(" USING TTL ?");
			values.add(ttl);
		}

		write(builder.toString(), values.toArray());
	}

	private void updateSecondaryIndexes(K key, Map<String, Object> row, Map<String, Long> timestamps, Map<String, Integer> ttls) {
		List<String> secondaryColumnsChanged = null;
		List<String> secondaryIndexByColumnNameChanges = null;

		// Gather list of secondary index columns that are being changed
		for (Map.Entry<String, Object> entry : row.entrySet()) {
			String column = entry.getKey();
			if (isSecondaryIndexByColumnNameAndValueEnabledForColumn(column)) {
				if (secondaryColumnsChanged == null) {
					// Lazy initialize to prevent wasting memory
					secondaryColumnsChanged = new ArrayList<>();
				}
				secondaryColumnsChanged.add(column);
			}
			if (isSecondaryIndexByColumnNameEnabledForColumn(column)) {
				if (secondaryIndexByColumnNameChanges == null) {
					// Lazy initialize to prevent wasting memory
					secondaryIndexByColumnNameChanges = new ArrayList<>();
				}
				secondaryIndexByColumnNameChanges.add(column);
			}
		}

		if (CollectionUtils.isNotEmpty(secondaryColumnsChanged)) {
			Map<String, String> oldValues = new HashMap<>();
			try {
				CassandraResultSet<K, String> readColumns = readColumns(key, secondaryColumnsChanged);
				for (String column : readColumns.getColumnNames()) {
					oldValues.put(column, readColumns.getString(column));
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			updateSecondaryIndexes(key, row, timestamps, ttls, secondaryColumnsChanged, oldValues);
		}

		if (CollectionUtils.isNotEmpty(secondaryIndexByColumnNameChanges)) {
			updateSecondaryIndexes(key, null, timestamps, ttls, secondaryIndexByColumnNameChanges, null);
		}
	}

	private void updateSecondaryIndexes(K key, String columnName, String value, long timestamp, int ttl) {
		if (isSecondaryIndexByColumnNameAndValueEnabledForColumn(columnName)) {
			String oldValue = readString(key, columnName);
			updateSecondaryIndexes(key, columnName, value, timestamp, ttl, oldValue);
		}

		if (isSecondaryIndexByColumnNameEnabledForColumn(columnName)) {
			updateSecondaryIndexes(key, columnName, "", timestamp, ttl, "");
		}
	}

	private void updateSecondaryIndexes(K key, Map<String, Object> row, Map<String, Long> timestamps, Map<String, Integer> ttls, List<String> columnsChanged,
			Map<String, String> oldValues) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		builder.append("BEGIN UNLOGGED BATCH\n");

		for (String columnName : columnsChanged) {
			String oldValue = oldValues != null ? oldValues.get(columnName) : null;
			if (!StringUtils.isBlank(oldValue) && !"null".equalsIgnoreCase(oldValue)) {
				// Delete old value if there is one (if it's null we'll just be writing it again down below with updated TS and TTL)
				builder.append("\tDELETE FROM " + secondaryIndexColumnFamily);

				Long timestamp = timestamps != null ? timestamps.get(columnName) : null;
				if (timestamp != null && timestamp > 0) {
					builder.append(" USING TIMESTAMP ?");
					values.add(timestamp);
				}

				builder.append(" where " + secondaryIndexKeyColumn + " = ? and column1 = ?;\n");
				values.add(getSecondaryIndexKey(columnName, oldValue));
				values.add(convertKey(key));
			}

			Object value = row != null ? row.get(columnName) : null;
			String valueToInsert = ClientManagerUtils.getInstance().convertValueForStorage(value);

			// Insert New Value
			builder.append("\tINSERT INTO " + secondaryIndexColumnFamily + " (" + secondaryIndexKeyColumn + ", column1, value) values (?,?,?)");
			values.add(getSecondaryIndexKey(columnName, valueToInsert));
			values.add(convertKey(key));
			values.add(convertKey(key));

			Long timestamp = timestamps != null ? timestamps.get(columnName) : null;
			Integer ttl = ttls != null ? ttls.get(columnName) : null;
			if (timestamp != null && timestamp > 0 && ttl != null && ttl > 0) {
				builder.append(" USING TIMESTAMP ? and TTL ?");
				values.add(timestamp);
				values.add(ttl);
			} else if (timestamp != null && timestamp > 0) {
				builder.append(" USING TIMESTAMP ?");
				values.add(timestamp);
			} else if (ttl != null && ttl > 0) {
				builder.append(" USING TTL ?");
				values.add(ttl);
			}
			builder.append(";\n");
		}

		builder.append("APPLY BATCH;");

		write(builder.toString(), values.toArray());
	}

	private void updateSecondaryIndexes(K key, String columnName, String value, long timestamp, int ttl, String oldValue) {
		StringBuilder builder = new StringBuilder();
		List<Object> values = new ArrayList<>();

		builder.append("BEGIN UNLOGGED BATCH\n");

		if (!StringUtils.isBlank(oldValue) && !"null".equalsIgnoreCase(oldValue)) {
			// Delete old value if there is one (if it's null we'll just be writing it again down below with updated TS and TTL)
			builder.append("\tDELETE FROM " + secondaryIndexColumnFamily);

			if (timestamp > 0) {
				builder.append(" USING TIMESTAMP ?");
				values.add(timestamp);
			}

			builder.append(" where " + secondaryIndexKeyColumn + " = ? and column1 = ?;\n");
			values.add(getSecondaryIndexKey(columnName, oldValue));
			values.add(convertKey(key));
		}

		// Insert New Value
		builder.append("\tINSERT INTO " + secondaryIndexColumnFamily + " (" + secondaryIndexKeyColumn + ", column1, value) values (?,?,?)");
		values.add(getSecondaryIndexKey(columnName, value));
		values.add(convertKey(key));
		values.add(convertKey(key));

		if (timestamp > 0 && ttl > 0) {
			builder.append(" USING TIMESTAMP ? and TTL ?");
			values.add(timestamp);
			values.add(ttl);
		} else if (timestamp > 0) {
			builder.append(" USING TIMESTAMP ?");
			values.add(timestamp);
		} else if (ttl > 0) {
			builder.append(" USING TTL ?");
			values.add(ttl);
		}
		builder.append(";\n");

		builder.append("APPLY BATCH;");

		write(builder.toString(), values.toArray());
	}

	private Object convertKey(K key) {
		if (keyType.getName() == Name.BIGINT) {
			return key;
		} else if (keyType.getName() == Name.TEXT) {
			return key.toString();
		} else {
			throw new RuntimeException("Unhandled DataType: " + keyType);
		}
	}

	private List<?> convertKeys(Set<K> keys) {
		if (keyType.getName() == Name.BIGINT) {
			return new ArrayList<>(keys);
		} else if (keyType.getName() != Name.TEXT) {
			throw new RuntimeException("Unhandled DataType: " + keyType);
		}

		List<Object> convertedKeys = new ArrayList<>(keys.size());

		for (K key : keys) {
			convertedKeys.add(key.toString());
		}

		return convertedKeys;
	}

	private CassandraResultSet<K, String> read(String query, Object... values) {
		return read(query, null, null, null, values);
	}

	private CassandraResultSet<K, String> read(String query, DataType keyType, DataType columnType, Map<String, DataType> valueTypes, Object... values) {
		logger.debug("query = {} : values = {}", query, values);
		PreparedStatement stmt = readStatementCache.getUnchecked(query);

		BoundStatement bind = stmt.bind(values);

		if (statementFetchSize > 0) {
			bind.setFetchSize(statementFetchSize);
		} else {
			bind.setFetchSize(Integer.MAX_VALUE);
		}

		long startTimeNanos = System.nanoTime();
		ResultSet rs = session.execute(bind);
		long durationNanos = System.nanoTime() - startTimeNanos;

		ExecutionInfo executionInfo = rs.getExecutionInfo();
		Host queriedHost = executionInfo.getQueriedHost();
		logger.debug("queried host = {}", queriedHost);

		if (tracingEnabled) {
			QueryTrace queryTrace = executionInfo.getQueryTrace();
			if (queryTrace != null) {
				if (logger.isDebugEnabled()) {
					logger.debug("{}", toString(queryTrace));
				}
			}
		}

		return new DataStaxCassandraResultSet<K>(rs, ObjectUtils.defaultIfNull(keyType, this.keyType), columnType, valueTypes, durationNanos);
	}

	private void write(String query, Object... values) {
		logger.debug("query = {} : values = {}", query, values);
		PreparedStatement stmt = writeStatementCache.getUnchecked(query);

		BoundStatement bind = stmt.bind(values);
		ResultSet rs = session.execute(bind);

		ExecutionInfo executionInfo = rs.getExecutionInfo();
		Host queriedHost = executionInfo.getQueriedHost();
		logger.debug("queried host = {}", queriedHost);

		if (tracingEnabled) {
			QueryTrace queryTrace = executionInfo.getQueryTrace();
			if (queryTrace != null) {
				if (logger.isDebugEnabled()) {
					logger.debug("{}", toString(queryTrace));
				}
			}
		}
	}

	private String toString(QueryTrace queryTrace) {
		StringBuilder builder = new StringBuilder();

		builder.append("Trace id: ").append(queryTrace.getTraceId());
		builder.append(String.format("%-38s | %-12s | %-10s | %-12s\n", "activity", "timestamp", "source", "source_elapsed"));
		builder.append("---------------------------------------+--------------+------------+--------------");

		for (QueryTrace.Event event : queryTrace.getEvents()) {
			builder.append(String.format("%38s | %12s | %10s | %12s\n", event.getDescription(), DATE_FORMATTER.print(event.getTimestamp()), event.getSource(),
					event.getSourceElapsedMicros()));
		}

		return builder.toString();
	}

	private void init() {
		LoadBalancingPolicy loadBalancingPolicy;
		if (datacenter != null) {
			loadBalancingPolicy = new DCAwareRoundRobinPolicy(datacenter);
		} else {
			loadBalancingPolicy = new RoundRobinPolicy();
		}
		loadBalancingPolicy = new TokenAwarePolicy(loadBalancingPolicy);
		loadBalancingPolicy = LatencyAwarePolicy.builder(loadBalancingPolicy).build();

		Builder builder = Cluster.builder().addContactPoints(endpoints).withLoadBalancingPolicy(loadBalancingPolicy);

		if (port > 0) {
			builder.withPort(port);
		}

		if (username != null && password != null) {
			builder.withCredentials(username, password);
		}

		if (compressionEnabled) {
			builder.withCompression(Compression.LZ4);
		}

		SocketOptions socketOptions = null;
		if (readTimeout > 0) {
			socketOptions = new SocketOptions();

			socketOptions.setReadTimeoutMillis(readTimeout);
		}

		if (connectTimeout > 0) {
			if (socketOptions == null) {
				socketOptions = new SocketOptions();
			}
			socketOptions.setConnectTimeoutMillis(connectTimeout);
		}

		if (socketOptions != null) {
			builder.withSocketOptions(socketOptions);
		}

		PoolingOptions poolingOptions = null;
		if (maxConnectionsPerHost > 0) {
			poolingOptions = new PoolingOptions();
			poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnectionsPerHost);
			poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnectionsPerHost);
		}

		if (poolingOptions != null) {
			builder.withPoolingOptions(poolingOptions);
		}

		cluster = builder.build();
		session = cluster.connect(keyspace);
	}

	@Override
	protected void logDownedHosts() {
	}
	
	@Override
	public void shutDown() {
		session.close();
		cluster.close();
	}
}
