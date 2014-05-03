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

import me.prettyprint.cassandra.service.CassandraHost;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * User: Eran C. Withana (eran.withana@wizecommerce.com) Date: 11/29/12
 */
public class HectorResultSetTest {

    private ColumnFamilyResult<Long, String> resultsSet;
    private HectorResultSet<Long, String> hectorResultSet;

    @SuppressWarnings("unchecked")
	@Before
    public void setup() {
        resultsSet = mock(ColumnFamilyResult.class);
        hectorResultSet = new HectorResultSet<Long, String>(resultsSet);
    }

    @Test
    public void testGetString() throws Exception {

        final String fieldName = "AwesomeColumn";
        final String stringFieldValue = "AwesomeValue";
        final String defaultStringFieldValue = "AwesomeDefaultValue";

        // this column has a value.
        when(resultsSet.getString(fieldName)).thenReturn(stringFieldValue);
        assertEquals(stringFieldValue, hectorResultSet.getString(fieldName));

        // this column doesn't have a value.
        when(resultsSet.getString(fieldName + "NotThere")).thenReturn("null");
        assertEquals(defaultStringFieldValue,
                     hectorResultSet.getString(fieldName + "NotThere", defaultStringFieldValue));
        assertNull(hectorResultSet.getString(fieldName + "NotThere"));

    }

    @Test
    public void testGetBoolean() throws Exception {
        final String fieldName = "AwesomeBooleanColumn";

        // this column has a value.
        when(resultsSet.getString(fieldName)).thenReturn("true");
        assertTrue(hectorResultSet.getBoolean(fieldName));

        when(resultsSet.getString(fieldName)).thenReturn("1");
        assertTrue(hectorResultSet.getBoolean(fieldName));

        when(resultsSet.getString(fieldName)).thenReturn("false");
        assertFalse(hectorResultSet.getBoolean(fieldName));

        // this column doesn't have a value.
        when(resultsSet.getString(fieldName + "NotThere")).thenReturn("null");
        assertFalse(hectorResultSet.getBoolean(fieldName + "NotThere", false));
        assertTrue(hectorResultSet.getBoolean(fieldName + "NotThere", true));
        assertFalse(hectorResultSet.getBoolean(fieldName + "NotThere"));

        // let this blow up.
        when(resultsSet.getString(fieldName)).thenReturn("ThisIsNotABoolean");
        assertEquals(false, hectorResultSet.getBoolean(fieldName));
    }

    @Test
    public void testGetDate() throws Exception {
        final String fieldName = "AwesomeDateColumn";

        // this column has a value.
        final Date actualDate = new GregorianCalendar(2012, 11, 29).getTime();
        when(resultsSet.getString(fieldName))
                .thenReturn(HecubaConstants.DATE_FORMATTER.print(actualDate.getTime()));
        assertEquals(actualDate.getTime(), hectorResultSet.getDate(fieldName).getTime());

        // this column doesn't have a value.
        when(resultsSet.getString(fieldName + "NotThere")).thenReturn("null");
        assertNull(hectorResultSet.getDate(fieldName + "NotThere"));

        final Date time = new GregorianCalendar(1978, 04, 16).getTime();
        assertEquals(time.getTime(), hectorResultSet.getDate(fieldName + "NotThere", time).getTime());

        // let this blow up.
        when(resultsSet.getString(fieldName)).thenReturn("ThisIsNotADate");
        assertNull(hectorResultSet.getDate(fieldName));
    }

    @Test
    public void testGetInteger() throws Exception {

        final String fieldName = "AwesomeIntegerColumn";

        // this column has a value.
        when(resultsSet.getString(fieldName)).thenReturn("1");
        assertEquals(1, (int) hectorResultSet.getInteger(fieldName));

        when(resultsSet.getString(fieldName)).thenReturn("100");
        assertEquals(100, (int) hectorResultSet.getInteger(fieldName));

        when(resultsSet.getString(fieldName)).thenReturn("-15");
        assertEquals(-15, (int) hectorResultSet.getInteger(fieldName));

        // this column doesn't have a value.
        when(resultsSet.getString(fieldName + "NotThere")).thenReturn("null");
        assertEquals(0, (int) hectorResultSet.getInteger(fieldName + "NotThere"));
        assertEquals(199, (int) hectorResultSet.getInteger(fieldName + "NotThere", 199));

        // let this blow up.
        when(resultsSet.getString(fieldName)).thenReturn("ThisIsNotAnInteger");
        assertEquals(0, (int) hectorResultSet.getInteger(fieldName));
        assertEquals(2211, (int) hectorResultSet.getInteger(fieldName, 2211));

    }


    @Test
    public void testGetLong() throws Exception {
        final String fieldName = "AwesomeLongColumn";

        // this column has a value.
        when(resultsSet.getString(fieldName)).thenReturn("1");
        assertEquals(1, (long) hectorResultSet.getLong(fieldName));

        when(resultsSet.getString(fieldName)).thenReturn("1000000000000000000");
        assertEquals(1000000000000000000L, (long) hectorResultSet.getLong(fieldName));

        when(resultsSet.getString(fieldName)).thenReturn("-15");
        assertEquals(-15L, (long) hectorResultSet.getLong(fieldName));

        // this column doesn't have a value.
        when(resultsSet.getString(fieldName + "NotThere")).thenReturn("null");
        assertEquals(0L, (long) hectorResultSet.getLong(fieldName + "NotThere"));
        assertEquals(2345L, (long) hectorResultSet.getLong(fieldName + "NotThere", 2345L));

        // let this blow up.
        when(resultsSet.getString(fieldName)).thenReturn("ThisIsNotAnInteger");
        assertEquals(0L, (long) hectorResultSet.getLong(fieldName));
        assertEquals(2211L, (long) hectorResultSet.getLong(fieldName, 2211L));
    }

    @Test
    public void testGetDouble() throws Exception {
        final String fieldName = "AwesomeDoubleColumn";

        // this column has a value.
        when(resultsSet.getString(fieldName)).thenReturn("0.33");
        assertEquals(0.33, hectorResultSet.getDouble(fieldName), 0.001);

        when(resultsSet.getString(fieldName)).thenReturn("12345.1234");
        assertEquals(12345.1234, hectorResultSet.getDouble(fieldName), 0.001);

        when(resultsSet.getString(fieldName)).thenReturn("-15.0");
        assertEquals(-15.0, hectorResultSet.getDouble(fieldName), 0.001);

        // this column doesn't have a value.
        when(resultsSet.getString(fieldName + "NotThere")).thenReturn("null");
        assertEquals(0.0, hectorResultSet.getDouble(fieldName + "NotThere"), 0.0);
        assertEquals(1.111, hectorResultSet.getDouble(fieldName + "NotThere", 1.111), 0.0);

        // let this blow up.
        when(resultsSet.getString(fieldName)).thenReturn("ThisIsNotADouble");
        assertEquals(0.0, hectorResultSet.getDouble(fieldName), 0.0);
        assertEquals(2211.0, hectorResultSet.getDouble(fieldName, 2211.0), 0.01);
    }


    @Test
    public void testGetFloat() throws Exception {
        final String fieldName = "AwesomeFloatColumn";

        // this column has a value.
        when(resultsSet.getString(fieldName)).thenReturn("0.33");
        assertEquals(0.33, hectorResultSet.getFloat(fieldName), 0.001);

        when(resultsSet.getString(fieldName)).thenReturn("12345.1234");
        assertEquals(12345.1234, hectorResultSet.getFloat(fieldName), 0.001);

        when(resultsSet.getString(fieldName)).thenReturn("-15.0");
        assertEquals(-15.0, hectorResultSet.getFloat(fieldName), 0.001);

        // this column doesn't have a value.
        when(resultsSet.getString(fieldName + "NotThere")).thenReturn("null");
        assertEquals(0.0, hectorResultSet.getFloat(fieldName + "NotThere"), 0.0);
        assertEquals(1.111, hectorResultSet.getFloat(fieldName + "NotThere", 1.111f), 0.001);

        // let this blow up.
        when(resultsSet.getString(fieldName)).thenReturn("ThisIsNotAFloat");
        assertEquals(0.0, hectorResultSet.getFloat(fieldName), 0.0);
        assertEquals(2211.0, hectorResultSet.getFloat(fieldName, 2211.0f), 0.01);
    }


    @Test
    public void testGetExecutionTimeMicro() throws Exception {
        when(resultsSet.getExecutionTimeMicro()).thenReturn(5555L);
        assertEquals(5555L, hectorResultSet.getExecutionTimeMicro());
    }

    @Test
    public void testGetExecutionTimeNano() throws Exception {
        when(resultsSet.getExecutionTimeNano()).thenReturn(1234567890L);
        assertEquals(1234567890L, hectorResultSet.getExecutionTimeNano());
    }

    @Test
    public void testGetHostUsed() throws Exception {
        when(resultsSet.getHostUsed()).thenReturn(new CassandraHost("cass1", 9160));
        final CassandraHost hostUsed = hectorResultSet.getHostUsed();
        assertEquals("cass1", hostUsed.getHost());
        assertEquals(9160, hostUsed.getPort());
    }

    @Test
    public void testGetColumnNames() throws Exception {
        String[] columnNames = new String[]{"Column1", "Column2", "Column3", "Column4"};

        when(resultsSet.getColumnNames()).thenReturn(Arrays.asList(columnNames));
        assertTrue(CollectionUtils.isEqualCollection(Arrays.asList(columnNames), hectorResultSet.getColumnNames()));
    }

    @Test
    public void testHasColumn() throws Exception {
        String[] columnNames = new String[]{"Column1", "Column2", "Column3", "Column4"};

        when(resultsSet.getColumnNames()).thenReturn(Arrays.asList(columnNames));
        for (String columnName : columnNames) {
            assertTrue(hectorResultSet.hasColumn(columnName));
        }
    }
}
