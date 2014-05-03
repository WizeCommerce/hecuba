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

import org.junit.Test;

import static junit.framework.Assert.assertEquals;


/**
 * User: Eran C. Withana (eran.withana@wizecommerce.com) Date: 11/29/12
 */
public class CassandraParamsBeanTest {
    @Test
    public void testDeepCopyWithSetValues() throws Exception {
        CassandraParamsBean initialBean = new CassandraParamsBean();
        initialBean.setColumnFamily("MyColumnFamily");
        initialBean.setClustername("My Awesome Cluster");
        initialBean.setKeyspace("MyAwesomeKeyspace");
        initialBean.setKeyType("UTF8Type");
        initialBean.setLocationURLs("www.wizecommerce.com");
        initialBean.setSiColumns("ColumnOne:ColumnTwo");
        initialBean.setThriftPorts("88990");
	    initialBean.setMaxSiColumnCount(1234);
	    initialBean.setMaxColumnCount(4321);
	    initialBean.setUsername("username");
	    initialBean.setPassword("secret");

        final CassandraParamsBean deepCopiedBean = new CassandraParamsBean(initialBean);
	    assertBeans(initialBean, deepCopiedBean);

    }

	@Test
	public void testCopyConstructor() throws Exception {
	    CassandraParamsBean initialBean = new CassandraParamsBean();
        initialBean.setColumnFamily("MyColumnFamily");
        initialBean.setClustername("My Awesome Cluster");
        initialBean.setKeyspace("MyAwesomeKeyspace");
        initialBean.setKeyType("UTF8Type");
        initialBean.setLocationURLs("www.wizecommerce.com");
        initialBean.setSiColumns("ColumnOne:ColumnTwo");
        initialBean.setThriftPorts("88990");
	    initialBean.setMaxSiColumnCount(1234);
	    initialBean.setMaxColumnCount(4321);
	    initialBean.setUsername("username");
	    initialBean.setPassword("secret");

	}

    @Test
    public void testDeepCopyWithDefaultValues() throws Exception {
        final   CassandraParamsBean initialBean = new CassandraParamsBean();

        final CassandraParamsBean deepCopiedBean = initialBean.deepCopy();
        assertEquals(initialBean.getColumnFamily(), deepCopiedBean.getColumnFamily());
        assertEquals(initialBean.getClustername(), deepCopiedBean.getClustername());
        assertEquals(initialBean.getKeyspace(), deepCopiedBean.getKeyspace());
        assertEquals(initialBean.getKeyType(), deepCopiedBean.getKeyType());
        assertEquals(initialBean.getLocationURLs(), deepCopiedBean.getLocationURLs());
        assertEquals(initialBean.getSiColumns(), deepCopiedBean.getSiColumns());
        assertEquals(initialBean.getThriftPorts(), deepCopiedBean.getThriftPorts());
	    assertEquals(initialBean.getMaxColumnCount(), deepCopiedBean.getMaxColumnCount());
	    assertEquals(initialBean.getMaxSiColumnCount(), deepCopiedBean.getMaxSiColumnCount());
	    assertEquals(initialBean.getUsername(), deepCopiedBean.getUsername());
	    assertEquals(initialBean.getPassword(), deepCopiedBean.getPassword());
    }

	private void assertBeans(CassandraParamsBean initialBean, CassandraParamsBean deepCopiedBean ) {
	    assertEquals(initialBean.getColumnFamily(), deepCopiedBean.getColumnFamily());
        assertEquals(initialBean.getClustername(), deepCopiedBean.getClustername());
        assertEquals(initialBean.getKeyspace(), deepCopiedBean.getKeyspace());
        assertEquals(initialBean.getKeyType(), deepCopiedBean.getKeyType());
        assertEquals(initialBean.getLocationURLs(), deepCopiedBean.getLocationURLs());
        assertEquals(initialBean.getSiColumns(), deepCopiedBean.getSiColumns());
        assertEquals(initialBean.getThriftPorts(), deepCopiedBean.getThriftPorts());
	    assertEquals(initialBean.getMaxColumnCount(), deepCopiedBean.getMaxColumnCount());
	    assertEquals(initialBean.getMaxSiColumnCount(), deepCopiedBean.getMaxSiColumnCount());
	    assertEquals(initialBean.getUsername(), deepCopiedBean.getUsername());
	    assertEquals(initialBean.getPassword(), deepCopiedBean.getPassword());
	}


}
