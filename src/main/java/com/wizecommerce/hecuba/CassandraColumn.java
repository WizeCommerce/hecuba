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

/**
 * This class will capture all the information related to a Cassandra column. 
 * 
 * User: Eran C. Withana (eran.withana@wizecommerce.com) Date: 12/7/12
 */
public class CassandraColumn {
    
    private Object name;
    private Object value;
    private long timestamp;
    private int ttl;

    public CassandraColumn(Object name, Object value, long timestamp, int ttl) {
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
        this.ttl = ttl;
    }

    public Object getName() {
        return name;
    }

    public void setName(Object name) {
        this.name = name;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }
}
