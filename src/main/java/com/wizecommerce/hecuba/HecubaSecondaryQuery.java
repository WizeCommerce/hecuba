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

import java.util.Map;

/**
 * User: Samir Faci
 * Date: 8/15/12
 * Time: 3:02 PM
 */

/**
 *  Object is used to query cassandra for secondary index.
 *
 *  @param parameters defines a map of columnname->value mapping that it should
 *  query against.
 *  @param limit defines the number of columns to return.
 *
 */
public class HecubaSecondaryQuery {
	final Map<String, Object> parameters;
	final Integer limit;

	public Integer getLimit() {
		return limit;
	}

	public Map getParameters() {
		return parameters;
	}

	public HecubaSecondaryQuery(Map map) {
		parameters = map;
		this.limit = Integer.MAX_VALUE;
	}

	public HecubaSecondaryQuery(Map map, Integer limit) {
		parameters = map;
		this.limit = limit;
	}

	@Override
	public String toString()
	{
		if(parameters == null)
			return "";
		else
		{
			StringBuffer buffer = new StringBuffer();
			for(Map.Entry<String, Object> entry : parameters.entrySet()) {
				buffer.append(entry.getKey() + ":::" + entry.getValue() + "\t");
			}
			return buffer.toString();

		}
	}
}
