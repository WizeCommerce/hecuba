package com.wizecommerce.hecuba.datastax;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions.Compression;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.wizecommerce.hecuba.datastax.SessionCachingKey.ClusterCachingKey;

/**
 * This class is to be used for internal purposes of hecuba client. This <b>singleton</b> implementation will
 * cache cluster and sessions according to datastax driver rules. i.e Use one Cluster instance per (physical)
 * cluster (per application lifetime). Use at most one Session per keyspace
 * 
 * @author anschauhan
 *
 */
@ThreadSafe
public class DataStaxBasedSessionObjectFactory {
	private Map<ClusterCachingKey, Cluster> clusterCache = null;

	private Map<SessionCachingKey, Session> sessionCache = null;
	private Map<SessionCachingKey, AtomicInteger> usersPerSession = null;

	// Initializing egarly since its very light weight, until clusters and session are built. Guarantees
	// thread safety.
	private static final DataStaxBasedSessionObjectFactory instance = new DataStaxBasedSessionObjectFactory();

	private DataStaxBasedSessionObjectFactory() {
		clusterCache = new HashMap<>();
		sessionCache = new HashMap<>();
		usersPerSession = new HashMap<>();
	}

	public static DataStaxBasedSessionObjectFactory getInstance() {
		return instance;
	}

	/**
	 * Call returnSession() everytime getSession() is called.
	 * 
	 * @param key
	 * @return
	 */
	public Session getSession(SessionCachingKey key) {
		Session session = sessionCache.get(key);
		if (session == null) {
			synchronized (sessionCache) {
				// Double check locking
				if (session == null) {
					Cluster cluster = getCluster(key.getClusterKey());
					session = cluster.connect(key.getKeySpace());
					sessionCache.put(key, session);
					usersPerSession.put(key, new AtomicInteger());
				}
			}
		}
		usersPerSession.get(key).incrementAndGet();
		return session;
	}

	public void returnSession(SessionCachingKey key) {
		int count = usersPerSession.get(key).decrementAndGet();
		if (count <= 0) {
			Session session = sessionCache.remove(key);
			// use different object to synchronize as this is independent from creation.
			synchronized (clusterCache) {
				if (session != null) {
					session.close();
				}
			}
		}
	}

	private Cluster getCluster(ClusterCachingKey key) {
		// No need of synchronizing here as getSession is already synchronized.
		Cluster cluster = clusterCache.get(key);
		if (cluster == null) {
			Map<String, Object> properties = key.getProperties();
			LoadBalancingPolicy loadBalancingPolicy;
			Object property = properties.get("dataCenter");
			if (property != null) {
				loadBalancingPolicy = new DCAwareRoundRobinPolicy((String) property);
			} else {
				loadBalancingPolicy = new RoundRobinPolicy();
			}
			loadBalancingPolicy = new TokenAwarePolicy(loadBalancingPolicy);
			loadBalancingPolicy = LatencyAwarePolicy.builder(loadBalancingPolicy).build();

			Builder builder = Cluster.builder().addContactPoints(key.getLocationUrls())
					.withLoadBalancingPolicy(loadBalancingPolicy);

			property = properties.get("port");
			if (property != null) {
				builder.withPort((Integer) property);
			}

			property = properties.get("username");
			if (property != null) {
				Object pass = properties.get("password");
				if (pass != null) {
					builder.withCredentials((String) property, (String) pass);
				}
			}

			property = properties.get("compressionEnabled");
			if ((Boolean) property) {
				builder.withCompression(Compression.LZ4);
			}

			SocketOptions socketOptions = null;
			property = properties.get("readTimeout");
			if (property != null) {
				socketOptions = new SocketOptions();

				socketOptions.setReadTimeoutMillis((Integer) property);
			}

			property = properties.get("connectTimeout");
			if (property != null) {
				if (socketOptions == null) {
					socketOptions = new SocketOptions();
				}
				socketOptions.setConnectTimeoutMillis((Integer) property);
			}

			if (socketOptions != null) {
				builder.withSocketOptions(socketOptions);
			}

			PoolingOptions poolingOptions = null;
			property = properties.get("maxConnectionsPerHost");
			if (properties != null) {
				poolingOptions = new PoolingOptions();
				Integer maxConnectionsPerHost = (Integer) property;
				poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, maxConnectionsPerHost);
				poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, maxConnectionsPerHost);
			}

			if (poolingOptions != null) {
				builder.withPoolingOptions(poolingOptions);
			}

			cluster = builder.build();
			clusterCache.put(key, cluster);
		}
		return cluster;
	}

}
