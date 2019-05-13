package de.komoot.photon.query;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.PrecisionModel;

import de.komoot.photon.utils.Function;

import spark.Request;

/**
 * Converts lon/lat parameter into location and validates the given coordinates.
 * Created by Holger Bruch on 10/13/2018.
 */
public class LocationParamConverter implements Function<Request, Point, BadRequestException> {

  private static final GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);
  private final boolean mandatory;

  public LocationParamConverter(final boolean mandatory) {
    this.mandatory = mandatory;
  }

  @Override
  public Point apply(final Request webRequest) throws BadRequestException {
    final Point location;
    final String lonParam = webRequest.queryParams("lon");
    final String latParam = webRequest.queryParams("lat");
    if (!mandatory && lonParam == null && latParam == null) {
      return null;
    }

    try {
      final Double lon = Double.valueOf(lonParam);
      if (lon > 180.0 || lon < -180.00) {
        throw new BadRequestException(400, "invalid search term 'lon', expected number >= -180.0 and <= 180.0");
      }
      final Double lat = Double.valueOf(latParam);
      if (lat > 90.0 || lat < -90.00) {
        throw new BadRequestException(400, "invalid search term 'lat', expected number >= -90.0 and <= 90.0");
      }
      location = geometryFactory.createPoint(new Coordinate(lon, lat));
    } catch (final NullPointerException | NumberFormatException e) {
      throw new BadRequestException(400, "invalid search term 'lat' and/or 'lon', try instead lat=51.5&lon=8.0");
    }
    return location;
  }
}
