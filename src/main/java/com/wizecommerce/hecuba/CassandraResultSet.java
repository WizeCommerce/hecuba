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
import java.util.UUID;

public interface CassandraResultSet<K, N> {

	public static final String HOST_NOT_AVAILABLE = "Not Available";

	/**
	 * Retrieves a string values from the column result. Note that we decided to
	 * push even the null values into Cassandra by encoding them as "null".
	 * Because of that we are explicitly checking the value of the retrieved
	 * value for "null" string so that we can return null objects.
	 */
	public abstract String getString(N fieldName);

	public abstract String getString(N fieldName, String defaultValue);

	public abstract Boolean getBoolean(N fieldName);

	public abstract Boolean getBoolean(N fieldName, Boolean defaultValue);

	public abstract Date getDate(N fieldName);

	public abstract Date getDate(N fieldName, Date defaultDate);

	public abstract java.sql.Date getSQLDate(N fieldName);

	public abstract java.sql.Date getSQLDate(N fieldName, java.sql.Date defaultDate);

	public abstract Timestamp getSQLTimeStamp(N fieldName);

	public abstract Timestamp getSQLTimeStamp(N fieldName, Timestamp defaultDate);

	public abstract Integer getInteger(N fieldName);

	public abstract Integer getInteger(N fieldName, int defaultInt);

	public abstract Integer getInteger(N fieldName, Integer defaultInt);

	public abstract Long getLong(N fieldName);

	public abstract Long getLong(N fieldName, long defaultLong);

	public abstract Long getLong(N fieldName, Long defaultLong);

	public abstract Double getDouble(N fieldName);

	public abstract Double getDouble(N fieldName, double defaultDouble);

	public abstract Double getDouble(N fieldName, Double defaultDouble);

	public abstract byte[] getByteArray(N fieldName);

	public abstract Collection<N> getColumnNames();

	public abstract Boolean hasColumn(String columnName);

	public abstract K getKey();

	public abstract UUID getUUID(N fieldName);

	public abstract boolean hasResults();

	public abstract Float getFloat(N fieldName);

	public abstract Float getFloat(N fieldName, Float defaultValue);

	public abstract boolean hasNextResult();

	public abstract void nextResult();

	/**
	 * This can be used to get the host used to run the last query. Will return "Not Available" if this informtion is
	 * not available.
	 *
	 * @return
	 */
	public abstract String getHost();

	/**
	 * This can be used to get the execution time for the last query. Will return -1 if this information is not
	 * available.
	 *
	 * @return
	 */
	public abstract long getExecutionLatency();

}
