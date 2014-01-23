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

import com.wizecommerce.hecuba.HecubaConstants;
import me.prettyprint.cassandra.model.HColumnImpl;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.beans.HColumn;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * This unit tests methods of HectorColumnSliceResultSet
 *
 * @author asinghal
 *         Date: Jan-2013
 */
public class HectorColumnSliceResultSetTest {

	private ColumnSlice<String, String> columnSlice;
	private HColumn<String, String> hColumn;
	private HectorColumnSliceResultSet<Long, String, String> sliceResultSet;
	private long timeInMillis;
	List<HColumn<String, String>> hColumns;
	List<String> columnNameList;

	@Before
	public void setup() {
		columnSlice = mock(ColumnSlice.class);

		hColumns = new ArrayList<HColumn<String, String>>();
		columnNameList = new ArrayList<String>();
		for (int i = 1; i <= 5; i++) {
			HColumn<String, String> hcol = new HColumnImpl<String, String>("Column" + i, "Value" + i,
																		   System.currentTimeMillis());
			hColumns.add(hcol);
			columnNameList.add("Column" + i);
		}
		when(columnSlice.getColumns()).thenReturn(hColumns);

		sliceResultSet = new HectorColumnSliceResultSet<Long, String, String>(columnSlice);
		timeInMillis = System.currentTimeMillis();
	}

	@Test
	public void testGetString() throws Exception {

		final String fieldName = "AwesomeColumn";
		final String stringFieldValue = "AwesomeValue";
		final String defaultStringFieldValue = "AwesomeDefaultValue";

		// this column has a value.
		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName,
																								stringFieldValue,
																								timeInMillis));
		assertEquals(stringFieldValue, sliceResultSet.getString(fieldName));

		// this column doesn't have a value.
		when(columnSlice.getColumnByName(fieldName + "NotThere")).thenReturn(null);
		assertEquals(defaultStringFieldValue,
					 sliceResultSet.getString(fieldName + "NotThere", defaultStringFieldValue));
		assertNull(sliceResultSet.getString(fieldName + "NotThere"));

	}

	@Test
	public void testGetBoolean() throws Exception {
		final String fieldName = "AwesomeBooleanColumn";

		// this column has a value.
		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName, "true",
																								timeInMillis));
		assertTrue(sliceResultSet.getBoolean(fieldName));

		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName, "1",
																								timeInMillis));
		assertTrue(sliceResultSet.getBoolean(fieldName));

		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName, "false",
																								timeInMillis));
		assertFalse(sliceResultSet.getBoolean(fieldName));

		// this column doesn't have a value.
		when(columnSlice.getColumnByName(fieldName + "NotThere")).thenReturn(null);
		assertFalse(sliceResultSet.getBoolean(fieldName + "NotThere", false));
		assertTrue(sliceResultSet.getBoolean(fieldName + "NotThere", true));
		assertFalse(sliceResultSet.getBoolean(fieldName + "NotThere"));

		// let this blow up.
		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName, "false",
																								timeInMillis));
		assertEquals(false, sliceResultSet.getBoolean(fieldName));
	}

	@Test
	public void testGetDate() throws Exception {
		final String fieldName = "AwesomeDateColumn";

		// this column has a value.
		final Date actualDate = new GregorianCalendar(2012, 11, 29).getTime();
		when(columnSlice.getColumnByName(fieldName)).thenReturn(
				new HColumnImpl<String, String>(fieldName, HecubaConstants.DATE_FORMATTER.
						print(actualDate.getTime()), timeInMillis));
		assertEquals(actualDate.getTime(), sliceResultSet.getDate(fieldName).getTime());

		// this column doesn't have a value.
		when(columnSlice.getColumnByName(fieldName + "NotThere")).thenReturn(null);
		assertNull(sliceResultSet.getDate(fieldName + "NotThere"));

		final Date time = new GregorianCalendar(1978, 04, 16).getTime();
		assertEquals(time.getTime(), sliceResultSet.getDate(fieldName + "NotThere", time).getTime());

		// let this blow up.
		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName,
																								"ThisIsNotADate",
																								timeInMillis));
		assertNull(sliceResultSet.getDate(fieldName));
	}

	@Test
	public void testGetInteger() throws Exception {

		final String fieldName = "AwesomeIntegerColumn";

		// this column has a value.
		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName, "1",
																								timeInMillis));
		assertEquals(1, (int) sliceResultSet.getInteger(fieldName));

		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName, "100",
																								timeInMillis));
		assertEquals(100, (int) sliceResultSet.getInteger(fieldName));

		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName, "-15",
																								timeInMillis));
		assertEquals(-15, (int) sliceResultSet.getInteger(fieldName));

		// this column doesn't have a value.
		when(columnSlice.getColumnByName(fieldName + "NotThere")).thenReturn(new HColumnImpl<String,
				String>(fieldName, "null", timeInMillis));
		assertEquals(0, (int) sliceResultSet.getInteger(fieldName + "NotThere"));
		assertEquals(199, (int) sliceResultSet.getInteger(fieldName + "NotThere", 199));

		// let this blow up.
		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName,
																								"ThisIsNotAnInteger",
																								timeInMillis));
		assertEquals(0, (int) sliceResultSet.getInteger(fieldName));
		assertEquals(2211, (int) sliceResultSet.getInteger(fieldName, 2211));

	}


	@Test
	public void testGetLong() throws Exception {
		final String fieldName = "AwesomeLongColumn";

		// this column has a value.
		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName, "1",
																								timeInMillis));
		assertEquals(1, (long) sliceResultSet.getLong(fieldName));

		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName,
																								"1000000000000000000",
																								timeInMillis));
		assertEquals(1000000000000000000L, (long) sliceResultSet.getLong(fieldName));

		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName, "-15",
																								timeInMillis));
		assertEquals(-15L, (long) sliceResultSet.getLong(fieldName));

		// this column doesn't have a value.
		when(columnSlice.getColumnByName(fieldName + "NotThere")).thenReturn(new HColumnImpl<String,
				String>(fieldName, "null", timeInMillis));
		assertEquals(0L, (long) sliceResultSet.getLong(fieldName + "NotThere"));
		assertEquals(2345L, (long) sliceResultSet.getLong(fieldName + "NotThere", 2345L));

		// let this blow up.
		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName,
																								"ThisIsNotAnInteger",
																								timeInMillis));
		assertEquals(0L, (long) sliceResultSet.getLong(fieldName));
		assertEquals(2211L, (long) sliceResultSet.getLong(fieldName, 2211L));
	}

	@Test
	public void testGetDouble() throws Exception {
		final String fieldName = "AwesomeDoubleColumn";

		// this column has a value.
		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName, "0.33",
																								timeInMillis));
		assertEquals(0.33, sliceResultSet.getDouble(fieldName), 0.001);

		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName,
																								"12345.1234",
																								timeInMillis));
		assertEquals(12345.1234, sliceResultSet.getDouble(fieldName), 0.001);

		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName, "-15.0",
																								timeInMillis));
		assertEquals(-15.0, sliceResultSet.getDouble(fieldName), 0.001);

		// this column doesn't have a value.
		when(columnSlice.getColumnByName(fieldName + "NotThere")).thenReturn(new HColumnImpl<String,
				String>(fieldName, "null", timeInMillis));
		assertEquals(0.0, sliceResultSet.getDouble(fieldName + "NotThere"), 0.0);
		assertEquals(1.111, sliceResultSet.getDouble(fieldName + "NotThere", 1.111), 0.0);

		// let this blow up.
		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName,
																								"ThisIsNotADouble",
																								timeInMillis));
		assertEquals(0.0, sliceResultSet.getDouble(fieldName), 0.0);
		assertEquals(2211.0, sliceResultSet.getDouble(fieldName, 2211.0), 0.01);
	}


	@Test
	public void testGetFloat() throws Exception {
		final String fieldName = "AwesomeFloatColumn";

		// this column has a value.
		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName, "0.33",
																								timeInMillis));
		assertEquals(0.33, sliceResultSet.getFloat(fieldName), 0.001);

		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName,
																								"12345.1234",
																								timeInMillis));
		assertEquals(12345.1234, sliceResultSet.getFloat(fieldName), 0.001);

		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName, "-15.0",
																								timeInMillis));
		assertEquals(-15.0, sliceResultSet.getFloat(fieldName), 0.001);

		// this column doesn't have a value.
		when(columnSlice.getColumnByName(fieldName + "NotThere")).thenReturn(new HColumnImpl<String,
				String>(fieldName, "null", timeInMillis));
		assertEquals(0.0, sliceResultSet.getFloat(fieldName + "NotThere"), 0.0);
		assertEquals(1.111, sliceResultSet.getFloat(fieldName + "NotThere", 1.111f), 0.001);

		// let this blow up.
		when(columnSlice.getColumnByName(fieldName)).thenReturn(new HColumnImpl<String, String>(fieldName,
																								"ThisIsNotAFloat",
																								timeInMillis));
		assertEquals(0.0, sliceResultSet.getFloat(fieldName), 0.0);
		assertEquals(2211.0, sliceResultSet.getFloat(fieldName, 2211.0f), 0.01);
	}

	@Test
	public void testGetColumnNames() throws Exception {


		assertTrue(CollectionUtils.isEqualCollection(columnNameList, sliceResultSet.getColumnNames()));
	}

	@Test
	public void testHasColumn() throws Exception {
		for (String columnName : columnNameList) {
			assertTrue(sliceResultSet.hasColumn(columnName));
		}
	}
}
