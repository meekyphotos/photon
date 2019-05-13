package de.komoot.photon;

import com.neovisionaries.i18n.CountryCode;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;

/**
 * denormalized doc with all information needed be dumped to elasticsearch
 *
 * @author christoph
 */
@Getter
@Setter
public class PhotonDoc {

  private final long placeId;
  private final String osmType;
  private final long osmId;
  private final String tagKey;
  private final String tagValue;
  private final Map<String, String> name;
  private final Map<String, String> extratags;
  private final Envelope bbox;
  private final long parentPlaceId; // 0 if unset
  private final double importance;
  private final CountryCode countryCode;
  private final long linkedPlaceId; // 0 if unset
  private final int rankSearch;
  private String postcode;
  private Map<String, String> street;
  private Map<String, String> city;
  private Set<Map<String, String>> context = new HashSet<Map<String, String>>();
  private Map<String, String> country;
  private Map<String, String> state;
  private String houseNumber;
  private Point centroid;

  public PhotonDoc(final long placeId, final String osmType, final long osmId, String tagKey, String tagValue, final Map<String, String> name, final String houseNumber, final Map<String, String> extratags, final Envelope bbox, final long parentPlaceId, final double importance, final CountryCode countryCode, final Point centroid, final long linkedPlaceId, final int rankSearch) {
    final String place = extratags != null ? extratags.get("place") : null;
    if (place != null) {
      // take more specific extra tag information
      tagKey = "place";
      tagValue = place;
    }

    this.placeId = placeId;
    this.osmType = osmType;
    this.osmId = osmId;
    this.tagKey = tagKey;
    this.tagValue = tagValue;
    this.name = name;
    this.houseNumber = houseNumber;
    this.extratags = extratags;
    this.bbox = bbox;
    this.parentPlaceId = parentPlaceId;
    this.importance = importance;
    this.countryCode = countryCode;
    this.centroid = centroid;
    this.linkedPlaceId = linkedPlaceId;
    this.rankSearch = rankSearch;
  }

  public PhotonDoc(final PhotonDoc other) {
    this.placeId = other.placeId;
    this.osmType = other.osmType;
    this.osmId = other.osmId;
    this.tagKey = other.tagKey;
    this.tagValue = other.tagValue;
    this.name = other.name;
    this.houseNumber = other.houseNumber;
    this.postcode = other.postcode;
    this.extratags = other.extratags;
    this.bbox = other.bbox;
    this.parentPlaceId = other.parentPlaceId;
    this.importance = other.importance;
    this.countryCode = other.countryCode;
    this.centroid = other.centroid;
    this.linkedPlaceId = other.linkedPlaceId;
    this.rankSearch = other.rankSearch;
    this.street = other.street;
    this.city = other.city;
    this.context = other.context;
    this.country = other.country;
    this.state = other.state;
  }

  /**
   * Used for testing - really all variables required (final)?
   */
  public static PhotonDoc create(final long placeId, final String osmType, final long osmId, final Map<String, String> nameMap) {
    return new PhotonDoc(placeId, osmType, osmId, "", "", nameMap,
                         "", null, null, 0, 0, null, null, 0, 0);
  }

  public String getUid() {
    if (houseNumber == null || houseNumber.isEmpty()) {
      return String.valueOf(placeId);
    } else {
      return placeId + "." + houseNumber;
    }
  }

  public boolean isUsefulForIndex() {
    if ("place".equals(tagKey) && "houses".equals(tagValue)) {
      return false;
    }

    if (houseNumber != null) {
      return true;
    }

    if (name.isEmpty()) {
      return false;
    }

    return linkedPlaceId <= 0;

  }
}
