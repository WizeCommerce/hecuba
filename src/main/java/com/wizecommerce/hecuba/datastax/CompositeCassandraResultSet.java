package com.wizecommerce.hecuba.datastax;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.wizecommerce.hecuba.AbstractCassandraResultSet;
import com.wizecommerce.hecuba.CassandraResultSet;

public class CompositeCassandraResultSet<K> extends AbstractCassandraResultSet<K, String> {
	private List<CassandraResultSet<K, String>> delegates;
	private CassandraResultSet<K, String> current;
	private Iterator<CassandraResultSet<K, String>> iterator;
	private boolean onFirstResult;

	public CompositeCassandraResultSet(List<CassandraResultSet<K, String>> delegates) {
		this.delegates = delegates;
		this.iterator = this.delegates.iterator();
		Preconditions.checkArgument(this.iterator.hasNext(), "delegates expected to be non-empty");
		current = iterator.next();
	}

	@Override
	public String getString(String fieldName) {
		return current.getString(fieldName);
	}

	@Override
	public byte[] getByteArray(String fieldName) {
		return current.getByteArray(fieldName);
	}

	@Override
	public Collection<String> getColumnNames() {
		return current.getColumnNames();
	}

	@Override
	public K getKey() {
		return current.getKey();
	}

	@Override
	public UUID getUUID(String fieldName) {
		return current.getUUID(fieldName);
	}

	@Override
	public boolean hasResults() {
		if (current.hasResults()) {
			return true;
		}
		while (iterator.hasNext()) {
			current = iterator.next();
			if (current.hasResults()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean hasNextResult() {
		if (current.hasNextResult()) {
			return true;
		}
		while (iterator.hasNext()) {
			current = iterator.next();
			if (current.hasResults()) {
				onFirstResult = true;
				return true;
			}
		}
		return false;
	}

	@Override
	public void nextResult() {
		if (!onFirstResult) {
			onFirstResult = false;
			current.nextResult();
		}
	}

	@Override
	public String getHost() {
		return current.getHost();
	}

	@Override
	public long getExecutionLatency() {
		return current.getExecutionLatency();
	}
}
