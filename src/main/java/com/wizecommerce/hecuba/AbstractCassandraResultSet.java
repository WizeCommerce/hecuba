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

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;

public abstract class AbstractCassandraResultSet<K, N> implements CassandraResultSet<K, N> {

	public final String getString(N fieldName, String defaultValue) {
		final String value = getString(fieldName);
		return value == null ? defaultValue : value;
	}

	@Override
	public final Boolean getBoolean(N fieldName) {
		return getBoolean(fieldName, false);
	}

	@Override
	public final Boolean getBoolean(N fieldName, Boolean defaultValue) {
		final String value = getString(fieldName);
		return value == null ? defaultValue : Boolean.valueOf("true".equalsIgnoreCase(value) || "1".equals(value));
	}

	@Override
	public final Date getDate(N fieldName) {
		return getDate(fieldName, null);
	}

	@Override
	public final Date getDate(N fieldName, Date defaultDate) {

		try {
			final String dateString = getString(fieldName);
			return dateString == null ? defaultDate : HecubaConstants.DATE_FORMATTER.parseDateTime(dateString).toDate();
		} catch (IllegalArgumentException e) {
			return defaultDate;
		}
	}

	@Override
	public final Integer getInteger(N fieldName) {
		return getInteger(fieldName, 0);
	}

	@Override
	public final Integer getInteger(N fieldName, int defaultInt) {
		try {
			final String value = getString(fieldName);
			return value == null ? defaultInt : Integer.parseInt(value);
		} catch (NumberFormatException e) {
			return defaultInt;
		}
	}

	@Override
	public final Integer getInteger(N fieldName, Integer defaultInt) {
		final String string = getString(fieldName);
		if (string == null) {
			return defaultInt;
		} else {
			return Integer.parseInt(string);
		}
	}

	@Override
	public final Long getLong(N fieldName) {
		return getLong(fieldName, 0);
	}

	@Override
	public final Long getLong(N fieldName, long defaultLong) {
		try {
			final String value = getString(fieldName);

			return value == null ? defaultLong : Long.parseLong(value);
		} catch (NumberFormatException e) {
			return defaultLong;
		}
	}

	@Override
	public final Long getLong(N fieldName, Long defaultLong) {
		final String string = getString(fieldName);
		if (string == null) {
			return defaultLong;
		} else {
			return Long.parseLong(string);
		}
	}

	@Override
	public final Double getDouble(N fieldName) {
		return getDouble(fieldName, 0);
	}

	@Override
	public final Double getDouble(N fieldName, double defaultDouble) {
		try {
			final String value = getString(fieldName);
			return value == null ? defaultDouble : Double.parseDouble(value);
		} catch (NumberFormatException e) {
			return defaultDouble;
		}
	}

	@Override
	public final Double getDouble(N fieldName, Double defaultDouble) {
		final String string = getString(fieldName);
		if (string == null) {
			return defaultDouble;
		} else {
			return Double.parseDouble(string);
		}
	}

	@Override
	public final Float getFloat(N fieldName) {
		return getFloat(fieldName, 0.0f);
	}

	@Override
	public final Float getFloat(N fieldName, Float defaultValue) {
		try {
			final String string = getString(fieldName);
			return string == null ? defaultValue : Float.parseFloat(string);
		} catch (NumberFormatException e) {
			return defaultValue;
		}

	}

	@Override
	public final Boolean hasColumn(String columnName) {
		Boolean result = false;
		Collection<N> allColumns = (Collection<N>) getColumnNames();

		for (N value : allColumns) {
			if (value.equals(columnName)) {
				result = true;
				break;
			}
		}
		return result;
	}

	@Override
	public final java.sql.Date getSQLDate(N fieldName) {
		Date java_date = getDate(fieldName);
		return (java_date == null) ? null : new java.sql.Date(java_date.getTime());
	}

	@Override
	public final java.sql.Date getSQLDate(N fieldName, java.sql.Date defaultDate) {
		return (getSQLDate(fieldName) != null) ? getSQLDate(fieldName) : defaultDate;
	}

	@Override
	public final Timestamp getSQLTimeStamp(N fieldName) {
		Date java_date = getDate(fieldName);
		return (java_date == null) ? null : new Timestamp(java_date.getTime());
	}

	@Override
	public final Timestamp getSQLTimeStamp(N fieldName, Timestamp defaultDate) {
		return (getSQLTimeStamp(fieldName) != null) ? getSQLTimeStamp(fieldName) : defaultDate;
	}
}
