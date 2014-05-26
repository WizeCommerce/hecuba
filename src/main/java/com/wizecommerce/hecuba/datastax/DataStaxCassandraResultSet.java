package com.wizecommerce.hecuba.datastax;

import java.net.InetAddress;
import java.util.*;

import org.apache.commons.lang3.builder.ToStringBuilder;

import com.datastax.driver.core.*;
import com.google.common.base.Objects;
import com.wizecommerce.hecuba.AbstractCassandraResultSet;

public class DataStaxCassandraResultSet<K> extends AbstractCassandraResultSet<K, String> {
	private ResultSet rs;
	private Iterator<Row> rowIterator;
	private DataType keyType;
	private DataType columnType;
	private Map<String, DataType> valueTypes = new HashMap<>();
	private Map<String, Object> currentRow = new LinkedHashMap<>();
	private Map<String, Object> nextRow = new LinkedHashMap<>();
	private K currentKey;
	private K nextKey;
	private long durationNanos;

	public DataStaxCassandraResultSet(ResultSet rs, DataType keyType, DataType columnType, Map<String, DataType> valueTypes, long durationNanos) {
		this.rs = rs;
		this.rowIterator = rs.iterator();
		this.durationNanos = durationNanos;
		this.keyType = keyType;
		this.columnType = columnType;
		this.valueTypes = valueTypes;

		extractRow();
	}

	@SuppressWarnings("unchecked")
	private void extractRow() {
		while (rowIterator.hasNext()) {
			Row row = rowIterator.next();

			K key = (K) getValue(row, "key", keyType);

			if (currentKey == null) {
				currentKey = key;
			}

			String column = getValue(row, "column1", columnType).toString();
			DataType valueType = null;
			if (valueTypes != null) {
				valueType = valueTypes.get(column);
				if (valueType == null) {
					valueType = valueTypes.get("*");
				}
			}
			Object value = getValue(row, "value", valueType);

			if (Objects.equal(key, currentKey)) {
				currentRow.put(column, value);
			} else {
				nextRow.put(column, value);
				nextKey = key;
				break;
			}
		}
	}

	private Object getValue(Row row, String column, DataType dataType) {
		if (dataType == null) {
			return row.getString(column);
		}

		switch (dataType.getName()) {
		case ASCII:
		case VARCHAR:
		case TEXT:
			return row.getString(column);
		case BIGINT:
		case COUNTER:
			return row.getLong(column);
		case BLOB:
			return row.getBytes(column);
		case BOOLEAN:
			return row.getBool(column);
		case DECIMAL:
			return row.getDecimal(column);
		case DOUBLE:
			return row.getDouble(column);
		case FLOAT:
			return row.getFloat(column);
		case INET:
			return row.getInet(column);
		case TIMESTAMP:
		case INT:
			return row.getInt(column);
		case TIMEUUID:
		case UUID:
			return row.getUUID(column);
		case VARINT:
			return row.getVarint(column);
		case CUSTOM:
			return row.getBytesUnsafe(column);
		default:
			throw new RuntimeException("Unhandled DataType: " + dataType);
		}
	}

	public boolean hasNext() {
		return nextKey != null;
	}

	public void next() {
		currentKey = nextKey;
		currentRow = nextRow;

		nextKey = null;
		nextRow = new LinkedHashMap<>();

		extractRow();
	}

	@Override
	public boolean hasNextResult() {
		return hasNext();
	}

	@Override
	public void nextResult() {
		next();
	}

	@Override
	public String getHost() {
		ExecutionInfo executionInfo = rs.getExecutionInfo();
		Host queriedHost = executionInfo.getQueriedHost();
		InetAddress address = queriedHost.getAddress();
		return address.getCanonicalHostName();
	}

	@Override
	public long getExecutionLatency() {
		return durationNanos;
	}

	@Override
	public K getKey() {
		return currentKey;
	}

	@Override
	public UUID getUUID(String columnName) {
		return (UUID) currentRow.get(columnName);
	}

	@Override
	public String getString(String columnName) {
		Object value = currentRow.get(columnName);

		if (value == null) {
			return null;
		}

		String result = value.toString();

		if ("null".equalsIgnoreCase(result)) {
			result = null;
		}

		return result;
	}

	@Override
	public byte[] getByteArray(String columnName) {
		String value = getString(columnName);
		if (value != null) {
			return value.getBytes();
		}
		return null;
	}

	@Override
	public Collection<String> getColumnNames() {
		return currentRow.keySet();
	}

	@Override
	public boolean hasResults() {
		return !currentRow.isEmpty();
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("key", currentKey).append("currentRow", currentRow).append("hasNextResult", hasNextResult()).toString();
	}
}
