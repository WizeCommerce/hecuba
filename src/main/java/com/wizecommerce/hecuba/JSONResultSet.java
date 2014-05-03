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

import org.json.simple.JSONObject;

import java.sql.Timestamp;
import java.util.Date;


public class JSONResultSet {

	final JSONObject decoder;

	private JSONResultSet() {

		decoder = null;
	}

	public JSONResultSet(JSONObject value) {

		this.decoder = value;
	}

	public String getString(String fieldName) {

		return getString(fieldName, "");
	}

	public String getString(String fieldName, String defaultValue) {
		String value = (String) decoder.get(fieldName);

		if ((value == null) || value.equals("")) {
			return defaultValue;
		}

		return value;
	}

	public Boolean getBoolean(String fieldName) {

		return getBoolean(fieldName, Boolean.FALSE);
	}

	public Boolean getBoolean(String fieldName, Boolean defaultValue) {

		Boolean value = (Boolean) decoder.get(fieldName);

		return (value == null) ? defaultValue : value;
	}

	public Integer getInteger(String fieldName) {

		return getInteger(fieldName, 0);
	}

	public Integer getInteger(String fieldName, Integer defaultInt) {

		Long value = (Long) decoder.get(fieldName);

		return (value == null) ? defaultInt : value.intValue();
	}

	public Long getLong(String fieldName) {

		return getLong(fieldName, 0L);
	}

	public Long getLong(String fieldName, Long defaultLong) {

		Long value = (Long) decoder.get(fieldName);

		return (value == null) ? defaultLong : value;

	}

	public Double getDouble(String fieldName) {

		return getDouble(fieldName, 0.0);
	}

	public Double getDouble(String fieldName, Double defaultDouble) {

		Double value = (Double) decoder.get(fieldName);

		return (value == null) ? defaultDouble : value;

	}

	public float getFloat(String fieldName) {
		return  getFloat(fieldName, 0.0f);
	}

	public float getFloat(String fieldName, Float defaultValue) {

		Float value = (Float) decoder.get(fieldName);

		return (value == null) ? defaultValue : value;
	}


	// numeric values seem to come in as Longs, need to downcast
	public Byte getByte(String fieldName) {
		final Long value = (Long) decoder.get(fieldName);

		return (value == null) ? null : value.byteValue();
	}


	/**
	 * you should be storing dates as longs
	 * 
	 * @param fieldName name of the field
	 * 
	 * @return the date associated with the fieldName 
	 */
	@Deprecated
	public Date getDate(String fieldName) {

		return getDate(fieldName, null);
	}


	/**
	 * NOTE: Using simple-json (our current implementation), date and time are considered complex objects. It is recommended to store the date in json as the epoch time ie.
	 * .getTime(); which should be available for java.util.Date, java.sql.Date, and java.sql.Timestamp;
	 * 
	 * @param fieldName name of the field
	 * @param defaultDate default date to return if no value assocated with the field
	 * 
	 * @return the associated with the fieldName or defaultDate if no value associated with the field
	 * 
	 **/
	@Deprecated
	public Date getDate(String fieldName, Date defaultDate) {

		Long epoch = (Long) decoder.get(fieldName);

		return (epoch == null) ? defaultDate : new Date(epoch);
	}

	@Deprecated
	public java.sql.Date getSQLDate(String fieldName) {

		return getSQLDate(fieldName, null);
	}

	@Deprecated
	public java.sql.Date getSQLDate(String fieldName,
		java.sql.Date defaultDate) {

		Long epoch = (Long) decoder.get(fieldName);

		return (epoch == null) ? defaultDate : new java.sql.Date(epoch);
	}

	@Deprecated
	public Timestamp getSQLTimeStamp(String fieldName) {

		return getSQLTimeStamp(fieldName, null);
	}

	@Deprecated
	public Timestamp getSQLTimeStamp(String fieldName, Timestamp defaultDate) {

		Long epoch = (Long) decoder.get(fieldName);

		return (epoch == null) ? defaultDate : new Timestamp(epoch);

	}

	public byte[] getByteArray(String fieldName) {

		byte[] value = (byte[]) decoder.get(fieldName);

		return value;
	}


}
