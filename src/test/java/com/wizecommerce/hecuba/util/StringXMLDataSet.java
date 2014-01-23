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

import org.cassandraunit.dataset.DataSet;
import org.cassandraunit.dataset.xml.AbstractXmlDataSet;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class StringXMLDataSet extends AbstractXmlDataSet implements DataSet {

	String xmlString;

	public StringXMLDataSet(String xmlString) {
		super(xmlString);
		this.xmlString = xmlString;
	}

	@Override
	protected InputStream getInputDataSetLocation(String arg0) {
		return new ByteArrayInputStream(arg0.getBytes());
	}

}
