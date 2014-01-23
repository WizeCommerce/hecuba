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

package com.wizecommerce.hecuba.astyanax;

import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.shallows.EmptyColumnList;
import com.wizecommerce.hecuba.AbstractCassandraResultSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;

public class AstyanaxResultSet<K, String> extends AbstractCassandraResultSet<K, String> {


	private ColumnList<String> columns;
	private Iterator<Row<K, String>> rowIterator;
	private K key;
	OperationResult<Rows<K, String>> result;
	Rows<K, String> rows;

	public AstyanaxResultSet(ColumnList<String> columns) {
		this.columns = columns;
	}

//	public AstyanaxResultSet(Rows<K, String> rows) {
//		this.rowIterator = rows.iterator();
//		Row<K, String> row = rowIterator.next();
//		this.columns = row.getColumns();
//		this.key = row.getKey();
//	}

	public AstyanaxResultSet(OperationResult<Rows<K, String>> result) {
		this.result = result;
		rows = result.getResult();
		this.rowIterator = rows.iterator();
		if (rowIterator.hasNext()) {
			Row<K, String> row = rowIterator.next();
			this.columns = row.getColumns();
			this.key = row.getKey();
		} else {
			this.columns = new EmptyColumnList<String>();
		}
	}

	/**
	 * Retrieves a string values from the column result. Note that we decided to push even the null values into
	 * Cassandra
	 * by encoding them as "null". Because of
	 * that we are explicitly checking the value of the retrieved value for "null" string so that we can return null
	 * objects.
	 */
	@Override
	public java.lang.String getString(String fieldName) {
		Column<String> columnValue = columns.getColumnByName(fieldName);

		if (columnValue == null) {
			return null;
		}

		java.lang.String value = columnValue.getStringValue();

		return "null".equalsIgnoreCase(value) ? null : value;
	}

	@Override
	public byte[] getByteArray(String fieldName) {
		if (hasColumn(fieldName.toString())) {
			return columns.getColumnByName(fieldName).getByteArrayValue();
		}
		return null;
	}

	@Override
	public Collection<String> getColumnNames() {
		Collection<String> columnNames = new ArrayList<String>();
		Iterator<Column<String>> columnIter = columns.iterator();
		while (columnIter.hasNext()) {
			Column<String> elem = columnIter.next();
			columnNames.add(elem.getName());
		}
		return columnNames;
	}

	@Override
	public UUID getUUID(String fieldName) {
		return columns.getColumnByName(fieldName).getUUIDValue();
	}

	@Override
	public boolean hasResults() {
		return !columns.isEmpty();
	}

	@Override
	public boolean hasNextResult() {
		return rowIterator.hasNext();
	}

	@Override
	public K getKey() {
		return this.key;
	}

	@Override
	public void nextResult() {
		Row<K, String> row = rowIterator.next();
		this.columns = row.getColumns();
		this.key = row.getKey();
	}

	@Override
	public java.lang.String getHost() {
		return result != null ? result.getHost().getHostName() : HOST_NOT_AVAILABLE;
	}


	@Override
	public long getExecutionLatency() {
		return result != null ? result.getLatency() : -1;
	}
}
