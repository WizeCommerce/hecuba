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


import com.google.common.base.Objects;

/**
 * This is a bean to store cassandra connection parameters.
 *
 * @author asinghal
 */
public class CassandraParamsBean {

	// Cassandra cluster parameters {"columnFamily", "keyspace", "thriftPorts",
	// "locationURLs", "clustername"};

	protected String clustername;
	protected String locationURLs;
	protected String thriftPorts;
	protected String keyspace;
	protected String columnFamily;
	protected String keyType;
	protected String username;
	protected String password;

	// secondary indexed columns. I shortened this on purpose to make the life of an admin, who will put these params in
	// using the configuration, easier.
	protected String siColumns;

	// maximum no of columns in cassandra row. This will be used if cassandra row contains > 100 columns
	// One such use case is ProductPriceHistory
	protected int maxColumnCount;
	// maximum no of columns in cassandra row. This will be used if cassandra secondary index contains more then 100 columns
	// One such use case is SellerProgram
	protected int maxSiColumnCount;

	// being a schema flexible datastore we sometimes need to create secondary indexes based on the names of the
	// columns. The following parameter will enable the secondary index columns. If you set this parameter,
	// we will match each and every column name with the given pattern and create a secondary index out of this.
	protected String siByColumnsPattern = null;


	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getKeyType() {
		return keyType;
	}

	public void setKeyType(String keyType) {
		this.keyType = keyType;
	}

	public String getClustername() {
		return clustername;
	}

	public void setClustername(String clustername) {
		this.clustername = clustername;
	}

	public String getLocationURLs() {
		return locationURLs;
	}

	public void setLocationURLs(String locationURLs) {
		this.locationURLs = locationURLs;
	}

	public String getThriftPorts() {
		return thriftPorts;
	}

	public void setThriftPorts(String thriftPorts) {
		this.thriftPorts = thriftPorts;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	@Deprecated
	public String getCf() {
		return columnFamily;
	}

	@Deprecated
	public void setCf(String cf) {
		this.columnFamily = cf;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public String getSiColumns() {
		return siColumns;
	}

	public void setSiColumns(String siColumns) {
		this.siColumns = siColumns;
	}

	public int getMaxColumnCount() {
		return maxColumnCount;
	}

	public void setMaxColumnCount(int maxColumnCount) {
		this.maxColumnCount = maxColumnCount;
	}

	public int getMaxSiColumnCount() {
		return maxSiColumnCount;
	}

	public void setMaxSiColumnCount(int maxSiColumnCount) {
		this.maxSiColumnCount = maxSiColumnCount;
	}


	public String toString() {
		return Objects.toStringHelper(this)
				.add("ClusterName", getClustername())
				.add("LocationURLs", getLocationURLs())
				.add("KeySpace,", getKeyspace())
				.add("ThriftPorts", getThriftPorts())
				.add("ColumnFamily", getColumnFamily())
				.add("KeyType", getKeyType())
				.add("SIColumns", getSiColumns())
				.add("MaxColumnCount", getMaxColumnCount())
				.add("MaxSiColumnCount", getMaxSiColumnCount())
				.add("username", getUsername())
//				.add("password", "*censored*")
				.toString();

	}

	public String getSiByColumnsPattern() {
		return siByColumnsPattern;
	}

	public void setSiByColumnsPattern(String siByColumnsPattern) {
		this.siByColumnsPattern = siByColumnsPattern;
	}

	public CassandraParamsBean() {
	}

	public CassandraParamsBean(CassandraParamsBean initialBean) {
		setClustername(clustername);
		setColumnFamily(columnFamily);
		setKeyspace(initialBean.keyspace);
		setLocationURLs(initialBean.locationURLs);
		setThriftPorts(initialBean.thriftPorts);
		setKeyType(initialBean.keyType);
		setSiColumns(initialBean.siColumns);
		setMaxColumnCount(initialBean.maxColumnCount);
		setMaxSiColumnCount(initialBean.maxSiColumnCount);
		setSiByColumnsPattern(initialBean.siByColumnsPattern);
		setUsername(initialBean.getUsername());
		setPassword(initialBean.getPassword());
	}


	@Deprecated
	public CassandraParamsBean deepCopy() {
		CassandraParamsBean cassandraParamsBean = new CassandraParamsBean();
		cassandraParamsBean.setClustername(clustername);
		cassandraParamsBean.setColumnFamily(columnFamily);
		cassandraParamsBean.setKeyspace(keyspace);
		cassandraParamsBean.setLocationURLs(locationURLs);
		cassandraParamsBean.setThriftPorts(thriftPorts);
		cassandraParamsBean.setKeyType(keyType);
		cassandraParamsBean.setSiColumns(siColumns);
		cassandraParamsBean.setMaxColumnCount(maxColumnCount);
		cassandraParamsBean.setMaxSiColumnCount(maxSiColumnCount);
		cassandraParamsBean.setSiByColumnsPattern(siByColumnsPattern);
		cassandraParamsBean.setPassword(password);
		cassandraParamsBean.setUsername(username);
		return cassandraParamsBean;
	}
}
