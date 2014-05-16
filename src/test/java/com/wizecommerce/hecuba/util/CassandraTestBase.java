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

package com.wizecommerce.hecuba.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.hector.api.Serializer;

import org.apache.axiom.om.*;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.DataLoader;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.wizecommerce.hecuba.CassandraParamsBean;
import com.wizecommerce.hecuba.HecubaClientManager;
import com.wizecommerce.hecuba.HecubaConstants;

public abstract class CassandraTestBase {
	@Rule
	public TestName testName = new TestName();

	// Need to wait for embedded cassandra server to start successfully
	private static final long cassandraServerWaitTime = TimeUnit.SECONDS.toMillis(5);
	// Default constants to be used with the default configuration files.
	public static final String CLUSTER_NAME = "TestCluster";
	public static final String LOCATION = "127.0.0.1";
	public static final String PORT = "9171";
	public static final String CQL_PORT = "9142";
	public static final String KEYSPACE = "NextagTest";
	public static final String DATACENTER = "datacenter1";
	public static final Serializer<Long> LONG_KEY_SERIALIZER = LongSerializer.get();

	protected static Logger logger = Logger.getLogger(CassandraTestBase.class);

	@Before
	public void setup() {
		try {
			ConfigUtils.getInstance().getConfiguration().setProperty(HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".consistencypolicy.read", "ONE");
			ConfigUtils.getInstance().getConfiguration().setProperty(HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".consistencypolicy.write", "ONE");

			// Find the test methods that you have in the sub class.
			List<String> columnFamilyNames = getColumnFamilies(testName.getMethodName());

			// now load this information into Cassandra cluster.
			EmbeddedCassandraServerHelper.startEmbeddedCassandra();
			if (cassandraServerWaitTime > 0L) {
				Thread.sleep(cassandraServerWaitTime);
			}

			DataLoader loader = new DataLoader(CLUSTER_NAME, LOCATION + ":" + PORT);
			loader.load(new StringXMLDataSet(createCassandraUnitConfigFile(columnFamilyNames)));
		} catch (ConfigurationException | TTransportException | IOException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@After
	public void after() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}

	/**
	 * CassandraUnit has its own configuration file that we can feed into it. This configuration file contains information such as the keyspace name, column names etc that we need
	 * to run the tests. When it loads this configuration file, CassandraUnit automatically creates those keyspaces and column families.
	 * 
	 * @param columnFamilyNames
	 * 
	 * @return
	 * 
	 * @throws java.io.FileNotFoundException
	 */
	public String createCassandraUnitConfigFile(List<String> columnFamilyNames) throws FileNotFoundException {
		try {
			OMFactory omFactory = OMAbstractFactory.getOMFactory();
			OMDocument omDocument = omFactory.createOMDocument();

			String cassandraUnitNamespace = "http://xml.dataset.cassandraunit.org";
			OMElement rootElement = omFactory.createOMElement(new QName("keyspace"));
			rootElement.declareDefaultNamespace(cassandraUnitNamespace);
			omDocument.setOMDocumentElement(rootElement);
			OMNamespace defaultNamespace = rootElement.getDefaultNamespace();

			// first set the keyspace name.
			OMElement keyspaceNameElement = omFactory.createOMElement("name", defaultNamespace, rootElement);
			keyspaceNameElement.setText(KEYSPACE);

			// add the column families.
			OMElement columnFamilies = omFactory.createOMElement("columnFamilies", defaultNamespace, rootElement);

			// In cases where the column value types of the column family is not "UTF8Type",
			// the column value type to be used instead will be available in this map, which is indexed by the name
			// of the column family.
			Map<String, String> columnValueTypeOverrides = getColumnValueTypeOverrides();
			boolean isColumnValueTypeOverridesAvailable = (columnValueTypeOverrides != null && columnValueTypeOverrides.size() > 0);

			for (String columnFamilyName : columnFamilyNames) {

				logger.info("Adding column family " + columnFamilyName + "...");

				final OMElement cfElement = omFactory.createOMElement("columnFamily", defaultNamespace, columnFamilies);

				final OMElement cfNameElement = omFactory.createOMElement("name", defaultNamespace, cfElement);
				cfNameElement.setText(columnFamilyName);

				boolean isSecondayIndexColumnFamily = columnFamilyName.endsWith(HecubaConstants.SECONDARY_INDEX_CF_NAME_SUFFIX);

				final OMElement keyTypeElement = omFactory.createOMElement("keyType", defaultNamespace, cfElement);
				keyTypeElement.setText(getKeyType(columnFamilyName));

				final OMElement comparatorTypeElement = omFactory.createOMElement("comparatorType", defaultNamespace, cfElement);
				comparatorTypeElement.setText(isSecondayIndexColumnFamily ? "LongType" : "UTF8Type");

				final OMElement defaultColumnValueTypeElement = omFactory.createOMElement("defaultColumnValueType", defaultNamespace, cfElement);

				String columnValueType = "UTF8Type";
				// If the column value type is overridden use that value
				if (isColumnValueTypeOverridesAvailable && columnValueTypeOverrides.containsKey(columnFamilyName)) {
					columnValueType = columnValueTypeOverrides.get(columnFamilyName);
				}
				defaultColumnValueTypeElement.setText(isSecondayIndexColumnFamily ? "LongType" : columnValueType);

				// Handle initial columnFamily data, if any.
				Map<String, Map<String, Object>> cfData = getData(columnFamilyName);
				if (cfData != null) {
					Iterator<String> keyIter = cfData.keySet().iterator();
					while (keyIter.hasNext()) {
						String key = (String) keyIter.next();

						final OMElement rowElement = omFactory.createOMElement("row", defaultNamespace, cfElement);
						final OMElement keyElement = omFactory.createOMElement("key", defaultNamespace, rowElement);
						keyElement.setText(key);

						Map<String, Object> nameValuePairs = cfData.get(key);
						for (String columnName : nameValuePairs.keySet()) {

							final OMElement columnElement = omFactory.createOMElement("column", defaultNamespace, rowElement);
							final OMElement nameElement = omFactory.createOMElement("name", defaultNamespace, columnElement);
							nameElement.setText(columnName);
							final OMElement valueElement = omFactory.createOMElement("value", defaultNamespace, columnElement);
							valueElement.setText(String.valueOf(nameValuePairs.get(columnName)));

						}

					}
				}

			}

			StringWriter stringWriter = new StringWriter();
			rootElement.serialize(stringWriter);
			String s = new String(stringWriter.getBuffer());
			logger.info("config = \n" + s);
			return s;

		} catch (XMLStreamException e) {
			e.printStackTrace();
		}
		return null;
	}

	protected String getKeyType(String columnName) {
		return columnName.endsWith(HecubaConstants.SECONDARY_INDEX_CF_NAME_SUFFIX) ? "UTF8Type" : "LongType";
	}

	/**
	 * Get the data for this column family. This is the opportunity for the test implementer to inject data into the given column family at the startup time. This way, he can
	 * assume this data is available when he starts running tests and will relieve him doing this during the runtime of the tests.
	 * 
	 * @param columnFamilyName
	 * 
	 * @return A map, indexed by the id of the element, which contains a map of column names and their values.
	 */
	protected abstract Map<String, Map<String, Object>> getData(String columnFamilyName);

	/**
	 * Get the names of those column families that should not have a corresponding secondary index column family.
	 * 
	 * @return A list with the names of the column families. Return null if none.
	 */
	protected abstract List<String> getSecondaryIndexExcludeList();

	/**
	 * Get the column value types to be used with a column family if it is not UTF8Type.
	 * 
	 * @return A map, indexed by the name of the column family. Return null if no such values are required.
	 */
	protected abstract Map<String, String> getColumnValueTypeOverrides();

	public List<String> getColumnFamilies(String testName) {
		List<String> columnFamilyNames = new ArrayList<>();
		List<String> excludeSecondaryIndexList = getSecondaryIndexExcludeList();

		columnFamilyNames.add(testName);

		// If there are no exclusions or this method is not excluded
		if (excludeSecondaryIndexList == null || (excludeSecondaryIndexList != null && !excludeSecondaryIndexList.contains(testName))) {
			// add a column family name to fit secondary indexes too.
			columnFamilyNames.add((testName + HecubaConstants.SECONDARY_INDEX_CF_NAME_SUFFIX));
		}

		// finaly add the name of the class also as a column family name.
		columnFamilyNames.add(this.getClass().getSimpleName());

		return columnFamilyNames;
	}

	protected abstract void tearDown();

	public abstract HecubaClientManager<Long> getHecubaClientManager(CassandraParamsBean paramsBean);

	/**
	 * @return CassandraParamsBean configured to read from the EmbeddedCassandraServer using the test's name as CF name
	 */
	public CassandraParamsBean getDefaultCassandraParamsBean() {
		CassandraParamsBean params = new CassandraParamsBean();

		params.setColumnFamily(testName.getMethodName());
		params.setClustername(CLUSTER_NAME);
		params.setLocationURLs(LOCATION);
		params.setThriftPorts(PORT);
		params.setCqlPort(CQL_PORT);
		params.setKeyspace(KEYSPACE);
		params.setDataCenter(DATACENTER);

		return params;
	}

	/**
	 * @return HecubaClientManager configured to read from the EmbeddedCassandraServer using the test's name as CF name
	 */
	public HecubaClientManager<Long> getHecubaClientManager() {
		CassandraParamsBean params = getDefaultCassandraParamsBean();

		return getHecubaClientManager(params);
	}
}
