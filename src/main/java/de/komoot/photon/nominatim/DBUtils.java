package de.komoot.photon.nominatim;

import com.google.common.collect.Maps;

import com.vividsolutions.jts.geom.Geometry;

import org.openstreetmap.osmosis.hstore.PGHStore;
import org.postgis.jts.JtsGeometry;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * utility functions to parse data from postgis
 *
 * @author christoph
 */
public class DBUtils {

  public static Map<String, String> getMap(final ResultSet rs, final String columnName) throws SQLException {
    final Map<String, String> tags = Maps.newHashMap();

    final PGHStore dbTags = (PGHStore) rs.getObject(columnName);
    if (dbTags != null) {
      for (final Map.Entry<String, String> tagEntry : dbTags.entrySet()) {
        tags.put(tagEntry.getKey(), tagEntry.getValue());
      }
    }

    return tags;
  }

  @Nullable
  public static <T extends Geometry> T extractGeometry(final ResultSet rs, final String columnName) throws SQLException {
    final JtsGeometry geom = (JtsGeometry) rs.getObject(columnName);
    if (geom == null) {
      //info("no geometry found in column " + columnName);
      return null;
    }
    return (T) geom.getGeometry();
  }
}
