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

/**
 *
 */
package com.wizecommerce.hecuba;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author - Eran Chinthaka Withana
 * @date - Sep 12, 2011
 */
public abstract class HecubaConstants {


	public static final String GLOBAL_PROP_NAME_PREFIX = "com.wizecommerce.hecuba";


	public static final String HECUBA_CASSANDRA_CLIENT_IMPLEMENTATION_MANAGER =
            GLOBAL_PROP_NAME_PREFIX + "hecuba.cassandraclientmanager";

    public static enum CassandraClientImplementation {
        HECTOR, ASTYANAX
    }

    public static DateTimeFormatter DATE_FORMATTER = DateTimeFormat.forPattern("E, dd MMM yyyy HH:mm:ss Z");

    /*****************************
     * Configuration Properties.
     *****************************/


    public static final String ASTYANAX_NODE_DISCOVERY_TYPE =
            GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.nodeDiscoveryType";
    public static final String ASTYANAX_CONNECTION_POOL_TYPE =
            GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.connectionPoolType";
    public static final String ASTYANAX_MAX_CONNS_PER_HOST =
            GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.maxConnsPerHost";
    public static final String ASTYANAX_LATENCY_AWARE_UPDATE_INTERVAL =
            GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.latencyAwareUpdateInterval";
    public static final String ASTYANAX_LATENCY_AWARE_RESET_INTERVAL =
            GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.latencyAwareResetInterval";
    public static final String ASTYANAX_LATENCY_AWARE_BADNESS_INTERVAL =
            GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.latencyAwareBadnessInterval";
    public static final String ASTYANAX_LATENCY_AWARE_WINDOW_SIZE =
            GLOBAL_PROP_NAME_PREFIX + ".client.astyanax.latencyAwareWindowSize";

    public static final String SECONDARY_INDEX_CF_NAME_SUFFIX = "_Secondary_Idx";

}
