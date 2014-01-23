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

package com.wizecommerce.hecuba.hector;

import me.prettyprint.cassandra.connection.DynamicLoadBalancingPolicy;
import me.prettyprint.cassandra.connection.LeastActiveBalancingPolicy;
import me.prettyprint.cassandra.connection.LoadBalancingPolicy;
import me.prettyprint.cassandra.connection.RoundRobinBalancingPolicy;

/**
 * These properties enable us to configure various capabilities of hector
 * clients (use to interact with Apache Cassandra)
 *
 * @author Eran Chinthaka Withana
 *
 */
public class HectorClientConfiguration {

    private int maxActive;
    private int maxIdle;
    private int thriftSocketTimeout;
    private boolean useThriftFramedTransport;
    private boolean retryDownedHosts;
    private int retryDownedHostsDelayInSeconds;
    private LoadBalancingPolicy loadBalancingPolicy;

    public int getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public int getThriftSocketTimeout() {
        return thriftSocketTimeout;
    }

    public void setThriftSocketTimeout(int thriftSocketTimeout) {
        this.thriftSocketTimeout = thriftSocketTimeout;
    }

    public boolean isUseThriftFramedTransport() {
        return useThriftFramedTransport;
    }

    public void setUseThriftFramedTransport(boolean useThriftTransport) {
        this.useThriftFramedTransport = useThriftTransport;
    }

    public boolean isRetryDownedHosts() {
        return retryDownedHosts;
    }

    public void setRetryDownedHosts(boolean retryDownedHosts) {
        this.retryDownedHosts = retryDownedHosts;
    }

    public int getRetryDownedHostsDelayInSeconds() {
        return retryDownedHostsDelayInSeconds;
    }

    public void setRetryDownedHostsDelayInSeconds(
            int retryDownedHostsDelayInSeconds) {
        this.retryDownedHostsDelayInSeconds = retryDownedHostsDelayInSeconds;
    }

    public LoadBalancingPolicy getLoadBalancingPolicy() {
        return loadBalancingPolicy;
    }

    public void setLoadBalancingPolicy(LoadBalancingPolicy loadBalancingPolicy) {
        this.loadBalancingPolicy = loadBalancingPolicy;
    }

    public void setLoadBalancingPolicy(String loadBalancingPolicy) {
        if (loadBalancingPolicy != null && !"".equals(loadBalancingPolicy)) {
            if ("LeastActiveBalancingPolicy".equals(loadBalancingPolicy)) {
                this.loadBalancingPolicy = new LeastActiveBalancingPolicy();
            } else if ("DynamicLoadBalancingPolicy".equals(loadBalancingPolicy)) {
                this.loadBalancingPolicy = new DynamicLoadBalancingPolicy();
            } else if ("RoundRobinBalancingPolicy".equals(loadBalancingPolicy)) {
                this.loadBalancingPolicy = new RoundRobinBalancingPolicy();
            }
        } else {
            this.loadBalancingPolicy = new RoundRobinBalancingPolicy();
        }
    }

}
