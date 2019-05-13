package de.komoot.photon.query;

import com.vividsolutions.jts.geom.Point;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A photon request that can hold filter parameters requested by client.
 * Created by Sachin Dole on 2/12/2015.
 */
public class FilteredPhotonRequest extends PhotonRequest {

  private final Set<String> excludeKeys = new HashSet<String>(3);
  private final Set<String> includeKeys = new HashSet<String>(3);
  private final Set<String> excludeValues = new HashSet<String>(3);
  private final Set<String> includeValues = new HashSet<String>();
  private final Map<String, Set<String>> includeTags = new HashMap<String, Set<String>>(3);
  private final Map<String, Set<String>> excludeTags = new HashMap<String, Set<String>>(3);
  private final Map<String, Set<String>> excludeTagValues = new HashMap<String, Set<String>>(3);

  FilteredPhotonRequest(final String query, final int limit, final Point locationForBias, final double locBiasScale, final String language) {
    super(query, limit, locationForBias, locBiasScale, language);
  }

  public Set<String> keys() {
    return includeKeys;
  }

  void keys(final String includeKey) {
    this.includeKeys.add(includeKey);
  }

  public Set<String> values() {
    return includeValues;
  }

  void values(final String keyToInclude) {
    includeValues.add(keyToInclude);
  }

  public Map<String, Set<String>> tags() {
    return includeTags;
  }

  void tags(final String aKey, final Set<String> manyValues) {
    this.includeTags.put(aKey, manyValues);
  }

  public Set<String> notValues() {
    return excludeValues;
  }

  public Map<String, Set<String>> notTags() {
    return excludeTags;
  }

  void notKeys(final String excludeKey) {
    excludeKeys.add(excludeKey);
  }

  void notTags(final String excludeKey, final Set<String> excludeManyValues) {
    excludeTags.put(excludeKey, excludeManyValues);
  }

  void notValues(final String excludeValue) {
    excludeValues.add(excludeValue);
  }

  public Set<String> notKeys() {
    return excludeKeys;
  }

  void tagNotValues(final String key, final Set<String> excludeValues) {
    excludeTagValues.put(key, excludeValues);
  }

  public Map<String, Set<String>> tagNotValues() {
    return excludeTagValues;

  }
}
