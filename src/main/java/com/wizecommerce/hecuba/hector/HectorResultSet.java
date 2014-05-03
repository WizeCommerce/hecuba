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

import com.wizecommerce.hecuba.AbstractCassandraResultSet;
import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.hector.api.beans.HColumn;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;

/**
 * The default implementation of ColumnFamilyResult exposes methods to retrieve column values as java types. For example, getBoolean will return the value as a
 * Boolean. But this works only if we set the proper column validator classes. In our design we decided to make all column values to be UTF8Type so all the
 * values retrieved will be strings. But to make the life easy we need to have the same convenience methods to retrieve params as known data types. This class
 * wraps the return ColumnFamilyResult and provides the nice interface devs would like to use.
 * 
 * @param <K> key type
 * @param <N> column type
 * @author ewithana
 */

public class HectorResultSet<K, N> extends AbstractCassandraResultSet<K, N> implements ColumnFamilyResult<K, N> {

	private ColumnFamilyResult<K, N> originalColFamResult;

	public HectorResultSet(ColumnFamilyResult<K, N> originalColFamResult) {
		this.originalColFamResult = originalColFamResult;
	}

	/**
	 * Retrieves a string values from the column result. Note that we decided to push even the null values into Cassandra by encoding them as "null". Because of
	 * that we are explicitly checking the value of the retrieved value for "null" string so that we can return null objects.
	 */
	@Override
	public String getString(N fieldName) {
		final String string = originalColFamResult.getString(fieldName);

		return "null".equalsIgnoreCase(string) ? null : string;
	}

	@Override
	public boolean hasNext() {
		return originalColFamResult.hasNext();
	}

	@Override
	public ColumnFamilyResult<K, N> next() {
		return originalColFamResult.next();
	}

	@Override
	public void remove() {
		originalColFamResult.remove();

	}

	@Override
	public long getExecutionTimeMicro() {
		return originalColFamResult.getExecutionTimeMicro();
	}

	@Override
	public long getExecutionTimeNano() {
		return originalColFamResult.getExecutionTimeNano();
	}

	@Override
	public CassandraHost getHostUsed() {
		return originalColFamResult.getHostUsed();
	}

	@Override
	public byte[] getByteArray(N fieldName) {
		return originalColFamResult.getByteArray(fieldName);
	}

	@Override
	public HColumn<N, ByteBuffer> getColumn(N fieldName) {
		return originalColFamResult.getColumn(fieldName);
	}

	@Override
	public Collection<N> getColumnNames() {
		return originalColFamResult.getColumnNames();
	}

	@Override
	public K getKey() {
		return originalColFamResult.getKey();
	}

	@Override
	public UUID getUUID(N fieldName) {
		return originalColFamResult.getUUID(fieldName);
	}

	@Override
	public boolean hasResults() {
		return originalColFamResult.hasResults();
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
		return originalColFamResult.getHostUsed().getHost();
	}

	@Override
	public long getExecutionLatency() {
		return originalColFamResult.getExecutionTimeMicro();
	}
}
