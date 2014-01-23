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
import me.prettyprint.hector.api.beans.Row;
import me.prettyprint.hector.api.beans.Rows;
import me.prettyprint.hector.api.query.QueryResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

/**
 * User: Eran C. Withana (eran.withana@wizecommerce.com)
 * Date: 4/2/13
 */
public class HectorRowSliceResultSet<K, N, V> extends AbstractCassandraResultSet<K, N> {
	private ColumnSlice<N, V> slice;
	private Row<K, N, V> row;
	private Iterator<Row<K, N, V>> iterator;
	private Collection<N> columnNames;
	private Rows<K, N, V> rowItems;
	QueryResult<Rows<K, N, V>> result;

	public HectorRowSliceResultSet(QueryResult<Rows<K, N, V>> result) {
		this.result = result;
		this.rowItems = result.get();
		iterator = rowItems.iterator();
		if (iterator == null) {
			return;
		}
		if (iterator.hasNext()) {
			row = iterator.next();
			if (row == null) {
				return;
			}
			this.slice = row.getColumnSlice();

		}

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
		if (iterator == null)
			return false;

		return iterator.hasNext();
	}

	@Override
	public void nextResult() {
		row = iterator.next();
		if (row != null) {
			this.slice = row.getColumnSlice();
			columnNames = null;
		}
	}

	@Override
	public String getHost() {
		return result.getHostUsed().getHost();
	}

	@Override
	public long getExecutionLatency() {
		return result.getExecutionTimeMicro();
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
		if (columnNames != null) {
			return columnNames;
		}

		if (slice == null || slice.getColumns() == null) {
			return null;
		}

		Collection<HColumn<N, V>> values = new ArrayList<HColumn<N, V>>(slice.getColumns());
		columnNames = new ArrayList<N>();
		for (HColumn<N, V> item : values) {
			columnNames.add(item.getName());
		}
		return columnNames;

	}

	@Override
	public K getKey() {
		if (row != null) {
			return row.getKey();
		}
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
