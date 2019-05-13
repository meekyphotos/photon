package de.komoot.photon.query;

import com.vividsolutions.jts.geom.Point;

import java.io.Serializable;

/**
 * Created by Sachin Dole on 2/12/2015.
 */
public class PhotonRequest implements Serializable {

  private final double scale;
  private final String query;
  private final Integer limit;
  private final Point locationForBias;
  private final String language;

  public PhotonRequest(final String query, final int limit, final Point locationForBias, final double scale, final String language) {
    this.query = query;
    this.limit = limit;
    this.locationForBias = locationForBias;
    this.scale = scale;
    this.language = language;
  }

  public String getQuery() {
    return query;
  }

  public Integer getLimit() {
    return limit;
  }

  public Point getLocationForBias() {
    return locationForBias;
  }

  public double getScaleForBias() {
    return scale;
  }

  public String getLanguage() {
    return language;
  }
}
