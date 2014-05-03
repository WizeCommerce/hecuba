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
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.query.QueryResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

/**
 * This implementation wraps {@link me.prettyprint.hector.api.beans.Rows} and {@link me.prettyprint.hector.api.beans.ColumnSlice} to read values from cassandra
 * 
 * @param <K> - Key type
 * @param <N> - Column name type
 * @param <V> - Column value type
 * 
 * User: Samir Faci Date: 8/14/12
 */
public class HectorColumnSliceResultSet<K, N, V> extends AbstractCassandraResultSet<K, N> {
	private ColumnSlice<N, V> slice;
	QueryResult<ColumnSlice<N, V>> queriedResult;
	private List<HColumn<N, V>> columnNames;


	public HectorColumnSliceResultSet(QueryResult<ColumnSlice<N, V>> queriedResult) {
		this.queriedResult = queriedResult;
		this.slice = queriedResult.get();
		this.columnNames = this.slice.getColumns();
	}

	public HectorColumnSliceResultSet(ColumnSlice columnSlice) {
		this.slice = columnSlice;
		this.columnNames = this.slice.getColumns();
	}

	@Override
	public String getString(N fieldName) {
		if (slice == null)
			return null;

		final HColumn<N, V> column = slice.getColumnByName(fieldName);
		if (column == null) {
			return null;
		}

		final String value = (String) column.getValue();
		return "null".equalsIgnoreCase(value) ? null : value;
	}

	@Override
	public boolean hasNextResult() {
		// column slice will always be for a single row.
		return false;
	}

	@Override
	public void nextResult() {
		// no nothing
	}

	@Override
	public String getHost() {
		return queriedResult.getHostUsed().getHost();
	}

	@Override
	public long getExecutionLatency() {
		return queriedResult.getExecutionTimeMicro();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.nextag.platform.datastore.com.wizecommerce.hecuba.CassandraResultSet#getByteArray(N)
	 */
	@Override
	public byte[] getByteArray(N fieldName) {
		return getString(fieldName).getBytes();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.nextag.platform.datastore.com.wizecommerce.hecuba.CassandraResultSet#getColumnNames()
	 */
	@Override
	public Collection<N> getColumnNames() {
		List<N> columnNameOnly = new ArrayList<N>();
		if (columnNames == null) {
			// try one more time.
			columnNames = slice.getColumns();
		}


		if (columnNames != null) {
			for (HColumn<N, V> column : columnNames) {
				columnNameOnly.add(column.getName());
			}
		}

		return columnNameOnly;
	}

	@Override
	public K getKey() {
		return null;
	}

	@Override
	public UUID getUUID(N fieldName) {
		return null;
	}

	@Override
	public boolean hasResults() {
		return slice != null && slice.getColumns() != null;
	}
}
