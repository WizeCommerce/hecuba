package com.wizecommerce.hecuba.datastax;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import com.google.common.base.Objects;

import edu.emory.mathcs.backport.java.util.Arrays;

/**
 * This class is to be used for internal purposes of Hecuba Client. Member variables are kept as generic as
 * possible such that it can be used as caching key for other cluster pools as well.
 * 
 * @author anschauhan
 *
 */
public class SessionCachingKey {

	private ClusterCachingKey clusterKey;

	private String keySpace;

	public SessionCachingKey(String[] locationUrls, String keySpace) {
		this(locationUrls, keySpace, null);
	}

	public SessionCachingKey(String[] locationUrls, String keySpace, Map<String, Object> properties) {
		this.clusterKey = new ClusterCachingKey(locationUrls, properties);
		this.keySpace = keySpace;
	}

	public String getKeySpace() {
		return keySpace;
	}

	public void setKeySpace(String keySpace) {
		this.keySpace = keySpace;
	}

	public ClusterCachingKey getClusterKey() {
		return clusterKey;
	}

	public void setClusterKey(ClusterCachingKey clusterKey) {
		this.clusterKey = clusterKey;
	}

	public void setClusterProperties(Map<String, Object> props) {
		if (props != null) {
			this.clusterKey.clusterProperties = props;
		}
	}

	@Override
	public boolean equals(Object o) {
		SessionCachingKey other;
		if (o instanceof SessionCachingKey) {
			other = (SessionCachingKey) o;
		} else {
			return false;
		}

		return keySpace.equals(other.keySpace) && clusterKey.equals(other.clusterKey);
	}

	@Override
	public int hashCode() {
			return clusterKey.hashCode() + 31 * keySpace.hashCode();
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("Cluster properties: ", clusterKey).add(" keySpace: ", keySpace)
				.toString();
	}

	public static class ClusterCachingKey {
		private String[] locationUrls;
		/**
		 * Optional properties. For each property that's different from last one, this key will change and
		 * will create a new cluster and new session, so input properties carefully. Do not input what's not
		 * necessary or might be transient or mutable. And since we are not expecting this to be used very
		 * frequently we are not caching hashCode.
		 */

		private Map<String, Object> clusterProperties;

		public ClusterCachingKey(String[] locationUrls) {
			this(locationUrls, null);
		}

		public ClusterCachingKey(String[] locationUrls, Map<String, Object> properties) {
			Arrays.sort(locationUrls);
			this.locationUrls = locationUrls;
			if (properties == null) {
				properties = new HashMap<String, Object>();
			}
			this.clusterProperties = properties;
		}

		public void addProperty(String name, Object value) {
			clusterProperties.put(name, value);
		}

		public String[] getLocationUrls() {
			return locationUrls;
		}

		public void setLocationUrls(String[] locationUrls) {
			this.locationUrls = locationUrls;
		}

		public Map<String, Object> getProperties() {
			return clusterProperties;
		}

		public void setProperties(Map<String, Object> properties) {
			this.clusterProperties = properties;
		}

		@Override
		public boolean equals(Object o) {
			ClusterCachingKey other;
			if (o instanceof ClusterCachingKey) {
				other = (ClusterCachingKey) o;
			} else {
				return false;
			}

			return new EqualsBuilder().append(clusterProperties, other.getProperties())
					.append(locationUrls, other.getLocationUrls()).isEquals();
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder().append(clusterProperties).append(locationUrls).toHashCode();
		}

		@Override
		public String toString() {
			return Objects.toStringHelper(this).add(" locationUrls: ", locationUrls)
					.add(" properties: ", clusterProperties).toString();
		}
	}
}
