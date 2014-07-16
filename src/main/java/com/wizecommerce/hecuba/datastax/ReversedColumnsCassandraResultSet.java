package com.wizecommerce.hecuba.datastax;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;

import com.google.common.collect.ImmutableList;
import com.wizecommerce.hecuba.CassandraResultSet;

/**
 * Reverses the columns of a result set. Order of keys is unchanged since it's a stream.
 * 
 * @param <K>
 *            Key Type of Result Set
 * @param <N>
 *            Value Type of Result Set
 */
public class ReversedColumnsCassandraResultSet<K, N> implements CassandraResultSet<K, N> {
	private CassandraResultSet<K, N> delegate;

	public ReversedColumnsCassandraResultSet(CassandraResultSet<K, N> delegate) {
		this.delegate = delegate;
	}

	@Override
	public String getString(N fieldName) {
		return delegate.getString(fieldName);
	}

	@Override
	public String getString(N fieldName, String defaultValue) {
		return delegate.getString(fieldName, defaultValue);
	}

	@Override
	public Boolean getBoolean(N fieldName) {
		return delegate.getBoolean(fieldName);
	}

	@Override
	public Boolean getBoolean(N fieldName, Boolean defaultValue) {
		return delegate.getBoolean(fieldName, defaultValue);
	}

	@Override
	public Date getDate(N fieldName) {
		return delegate.getDate(fieldName);
	}

	@Override
	public Date getDate(N fieldName, Date defaultDate) {
		return delegate.getDate(fieldName, defaultDate);
	}

	@Override
	public java.sql.Date getSQLDate(N fieldName) {
		return delegate.getSQLDate(fieldName);
	}

	@Override
	public java.sql.Date getSQLDate(N fieldName, java.sql.Date defaultDate) {
		return delegate.getSQLDate(fieldName, defaultDate);
	}

	@Override
	public Timestamp getSQLTimeStamp(N fieldName) {
		return delegate.getSQLTimeStamp(fieldName);
	}

	@Override
	public Timestamp getSQLTimeStamp(N fieldName, Timestamp defaultDate) {
		return delegate.getSQLTimeStamp(fieldName, defaultDate);
	}

	@Override
	public Integer getInteger(N fieldName) {
		return delegate.getInteger(fieldName);
	}

	@Override
	public Integer getInteger(N fieldName, int defaultInt) {
		return delegate.getInteger(fieldName, defaultInt);
	}

	@Override
	public Integer getInteger(N fieldName, Integer defaultInt) {
		return delegate.getInteger(fieldName, defaultInt);
	}

	@Override
	public Long getLong(N fieldName) {
		return delegate.getLong(fieldName);
	}

	@Override
	public Long getLong(N fieldName, long defaultLong) {
		return delegate.getLong(fieldName, defaultLong);
	}

	@Override
	public Long getLong(N fieldName, Long defaultLong) {
		return delegate.getLong(fieldName, defaultLong);
	}

	@Override
	public Double getDouble(N fieldName) {
		return delegate.getDouble(fieldName);
	}

	@Override
	public Double getDouble(N fieldName, double defaultDouble) {
		return delegate.getDouble(fieldName, defaultDouble);
	}

	@Override
	public Double getDouble(N fieldName, Double defaultDouble) {
		return delegate.getDouble(fieldName, defaultDouble);
	}

	@Override
	public byte[] getByteArray(N fieldName) {
		return delegate.getByteArray(fieldName);
	}

	@Override
	public Collection<N> getColumnNames() {
		return ImmutableList.copyOf(delegate.getColumnNames()).reverse();
	}

	@Override
	public Boolean hasColumn(String columnName) {
		return delegate.hasColumn(columnName);
	}

	@Override
	public K getKey() {
		return delegate.getKey();
	}

	@Override
	public UUID getUUID(N fieldName) {
		return delegate.getUUID(fieldName);
	}

	@Override
	public boolean hasResults() {
		return delegate.hasResults();
	}

	@Override
	public Float getFloat(N fieldName) {
		return delegate.getFloat(fieldName);
	}

	@Override
	public Float getFloat(N fieldName, Float defaultValue) {
		return delegate.getFloat(fieldName, defaultValue);
	}

	@Override
	public boolean hasNextResult() {
		return delegate.hasNextResult();
	}

	@Override
	public void nextResult() {
		delegate.nextResult();
	}

	@Override
	public String getHost() {
		return delegate.getHost();
	}

	@Override
	public long getExecutionLatency() {
		return delegate.getExecutionLatency();
	}
}
