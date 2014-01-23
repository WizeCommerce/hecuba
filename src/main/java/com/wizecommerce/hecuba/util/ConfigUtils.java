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

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * This will be the central point to hold all the configuration needed during startup and runtime. Application needs
 * to first load all the properties into this by calling getInstance(Configuration) method. And, within the code,
 * at any place, properties can be easily accessed by getting a handle into this class.
 *
 * Future enhancements: this class can listen to changes in a configuration server (e.g. Zookeeper) and update
 * properties accordingly. This way, the changes happening at a central location can be propagated to the
 * application easily.
 *
 * list of properties:
 *
 * <h3>General Properties</h3>
 * <table>
 *     <tr>
 *         <th>Property</th>
 *         <th>Default Value</th>
 *         <th>Description</th>
 *     </tr>
 *
 *     <tr>
 *         <td>GLOBAL_PROP_NAME_PREFIX + "hecuba.cassandraclientmanager"</td>
 *         <td>HECTOR</td>
 *         <td>The cassandra client manager to be used. For now, we support hector and Astyanax and by default we
 *         select hector</td>
 *     </tr>
 *
 *     <tr>
 *         <td>HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".hecuba.consistencypolicy.read"</td>
 *         <td>ONE</td>
 *         <td>the consistency level to be used during a read operation.</td>
 *     </tr>
 *
 *     <tr>
 *          <td>HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".hecuba.consistencypolicy.write"</td>
 *         <td>ONE</td>
 *         <td>the consistency level to be used during a write operation.</td>
 *     </tr>
 *
 *     <tr>
 *         <td>HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".hecuba.path.separator"</td>
 *         <td>:</td>
 *         <td>used to separate the list of cassandra nodes we should be connecting to</td>
 *     </tr>
 *
 *     <tr>
 *         <td>HecubaConstants.GLOBAL_PROP_NAME_PREFIX + ".hectorpools.enabledebugmessages"</td>
 *         <td>false</td>
 *         <td>Enables debug messages coming. By default we enable only INFO messages</td>
 *     </tr>
 *
 *     <tr>
 *         <td>HecubaConstants.GLOBAL_PROP_NAME_PREFIX + "." + columnFamily + ".secondaryIndexCF"</td>
 *         <td>columnFamilyName + "_Secondary_Idx"</td>
 *         <td>within this implementation we maintain our own secondary index column families. This parameter is
 *         used to provide the name of the secondary index CF. This CF must have already been created to be used
 *         here. </td>
 *     </tr>
 *
 * </table>
 *
 * <h3>Hector Specific Properties</h3>
 * <table>
 *
 * </table>
 *
 * <h3>Astyanax Specific Properties</h3>
 *
 * <table>
 *    <tr>
 *        <td>GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.nodeDiscoveryType"</td>
 *        <td>RING_DESCRIBE</td>
 *        <td></td>
 *    </tr>
 *    <tr>
 *        <td>GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.connectionPoolType"</td>
 *        <td></td>
 *        <td></td>
 *    </tr>
 *    <tr>
 *        <td>GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.maxConnsPerHost"</td>
 *        <td></td>
 *        <td></td>
 *    </tr>
 *    <tr>
 *        <td>GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.latencyAwareUpdateInterval"</td>
 *        <td></td>
 *        <td></td>
 *    </tr>
 *    <tr>
 *        <td>GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.latencyAwareResetInterval"</td>
 *        <td></td>
 *        <td></td>
 *    </tr>
 *    <tr>
 *        <td>GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.latencyAwareBadnessInterval"</td>
 *        <td></td>
 *        <td></td>
 *    </tr>
 *    <tr>
 *        <td>GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.latencyAwareWindowSize"</td>
 *        <td></td>
 *        <td></td>
 *    </tr>
 *
 * </table>
 *
 * User: Eran Chinthaka Withana
 * Date: 12/31/13
 */
public class ConfigUtils {
	private static ConfigUtils ourInstance = new ConfigUtils();

	private static Configuration mainConfiguration = new CompositeConfiguration();

	public static ConfigUtils getInstance() {
		return ourInstance;
	}

	public static ConfigUtils getInstance(Configuration configuration) {
		mainConfiguration = configuration;
		return ourInstance;
	}

	private ConfigUtils() {
	}

	public Configuration getConfiguration() {
		return mainConfiguration;
	}
}
