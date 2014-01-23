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

import org.apache.commons.codec.binary.Base64;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

public class CassandraMapResultSet<K, N> extends AbstractCassandraResultSet<K, N>{
	private Map<String, String> map;
	
	public CassandraMapResultSet(Map<String, String> map) {
		this.map = map;
	}

	@Override
	public String getString(N fieldName) {
		return map.get(fieldName);
	}

	@Override
	public byte[] getByteArray(N fieldName) {
		Base64 base64 = new Base64();
		return base64.decode(getString(fieldName));
	}

	@Override
	@SuppressWarnings("unchecked")
	public Collection<N> getColumnNames() {
		return (Collection<N>) map.keySet();
	}

	@Override
	public K getKey() {
		return null;
	}

	@Override
	public UUID getUUID(N fieldName) {
		return null;
	}

	@Override
	public boolean hasResults() {
		return !map.isEmpty();
	}

	@Override
	public boolean hasNextResult() {
		return false;
	}

	@Override
	public void nextResult() {
	}

	@Override
	public String getHost() {
		return null;
	}

	@Override
	public long getExecutionLatency() {
		return -1L;
	}

}
