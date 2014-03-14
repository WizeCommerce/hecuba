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

public class ColumnFamilyInfo {

	private String name;
	private String comparatorType = "UTF8Type";
	private String keyValidationClass = "LongType";
	private String defaultValidationClass = "UTF8Type";

	public ColumnFamilyInfo(String name) {
		this.name = name;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getComparatorType() {
		return comparatorType;
	}

	public void setComparatorType(String coparatorType) {
		this.comparatorType = coparatorType;
	}

	public String getKeyValidationClass() {
		return keyValidationClass;
	}

	public void setKeyValidationClass(String keyValidationClass) {
		this.keyValidationClass = keyValidationClass;
	}

	public String getDefaultValidationClass() {
		return defaultValidationClass;
	}

	public void setDefaultValidationClass(String defaultValidationClass) {
		this.defaultValidationClass = defaultValidationClass;
	}

	public String toString() {
		return Objects.toStringHelper(this)
				.add("Name", name)
				.add("ComparatorType", comparatorType)
				.add("KeyValidationClass", keyValidationClass)
				.add("DefaultValidationClass", defaultValidationClass)
				.toString();
	}

}
