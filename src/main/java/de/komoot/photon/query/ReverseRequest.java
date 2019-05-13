package de.komoot.photon.query;

import com.vividsolutions.jts.geom.Point;

import java.io.Serializable;

/**
 * @author svantulden
 */
public class ReverseRequest implements Serializable {

  private final Point location;
  private final String language;
  private final Double radius;
  private final Integer limit;
  private final String queryStringFilter;
  private Boolean locationDistanceSort = true;

  public ReverseRequest(final Point location, final String language, final Double radius, final String queryStringFilter, final Integer limit, final Boolean locationDistanceSort) {
    this.location = location;
    this.language = language;
    this.radius = radius;
    this.limit = limit;
    this.queryStringFilter = queryStringFilter;
    this.locationDistanceSort = locationDistanceSort;
  }

  public Point getLocation() {
    return location;
  }

  public String getLanguage() {
    return language;
  }

  public Double getRadius() {
    return radius;
  }

  public Integer getLimit() {
    return limit;
  }

  public String getQueryStringFilter() {
    return queryStringFilter;
  }

  public Boolean getLocationDistanceSort() {
    return locationDistanceSort;
  }
}
