package de.komoot.photon.nominatim;

import com.google.common.collect.ImmutableList;

import com.neovisionaries.i18n.CountryCode;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.linearref.LengthIndexedLine;

import de.komoot.photon.Importer;
import de.komoot.photon.PhotonDoc;
import de.komoot.photon.nominatim.model.AddressRow;

import org.apache.commons.dbcp.BasicDataSource;
import org.postgis.jts.JtsWrapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

import lombok.extern.slf4j.Slf4j;

/**
 * A Nominatim result consisting of the basic PhotonDoc for the object
 * and a map of attached house numbers together with their respective positions.
 */
class NominatimResult {

  private final PhotonDoc doc;
  private Map<String, Point> housenumbers;

  public NominatimResult(final PhotonDoc baseobj) {
    doc = baseobj;
    housenumbers = null;
  }

  PhotonDoc getBaseDoc() {
    return doc;
  }

  boolean isUsefulForIndex() {
    return housenumbers != null && !housenumbers.isEmpty() || doc.isUsefulForIndex();
  }

  List<PhotonDoc> getDocsWithHousenumber() {
    if (housenumbers == null || housenumbers.isEmpty()) {
      return ImmutableList.of(doc);
    }

    final List<PhotonDoc> results = new ArrayList<PhotonDoc>(housenumbers.size());
    for (final Map.Entry<String, Point> e : housenumbers.entrySet()) {
      final PhotonDoc copy = new PhotonDoc(doc);
      copy.setHouseNumber(e.getKey());
      copy.setCentroid(e.getValue());
      results.add(copy);
    }

    return results;
  }

  /**
   * Adds house numbers from a house number string.
   * <p>
   * This may either be a single house number or multiple
   * house numbers delimited by a semicolon. All locations
   * will be set to the centroid of the doc geometry.
   *
   * @param str House number string. May be null, in which case nothing is added.
   */
  public void addHousenumbersFromString(final String str) {
    if (str == null || str.isEmpty()) {
      return;
    }

    if (housenumbers == null) {
      housenumbers = new HashMap<String, Point>();
    }

    final String[] parts = str.split(";");
    for (final String part : parts) {
      final String h = part.trim();
      if (!h.isEmpty()) {
        housenumbers.put(h, doc.getCentroid());
      }
    }
  }

  public void addHouseNumbersFromInterpolation(final long first, final long last, final String interpoltype, final Geometry geom) {
    if (last <= first || last - first > 1000) {
      return;
    }

    if (housenumbers == null) {
      housenumbers = new HashMap<String, Point>();
    }

    final LengthIndexedLine line = new LengthIndexedLine(geom);
    final double si = line.getStartIndex();
    final double ei = line.getEndIndex();
    final double lstep = (ei - si) / (double) (last - first);

    // leave out first and last, they have a distinct OSM node that is already indexed
    long step = 2;
    long num = 1;
    if (interpoltype.equals("odd")) {
      if (first % 2 == 1) {
        ++num;
      }
    } else if (interpoltype.equals("even")) {
      if (first % 2 == 0) {
        ++num;
      }
    } else {
      step = 1;
    }

    final GeometryFactory fac = geom.getFactory();
    for (; first + num < last; num += step) {
      housenumbers.put(String.valueOf(num + first), fac.createPoint(line.extractPoint(si + lstep * num)));
    }
  }
}

/**
 * Export nominatim data
 *
 * @author felix, christoph
 */
@Slf4j
public class NominatimConnector {

  private static final PhotonDoc FINAL_DOCUMENT = new PhotonDoc(0, null, 0, null, null, null, null, null, null, 0, 0, null, null, 0, 0);
  private final JdbcTemplate template;
  private final String selectColsPlaceX = "place_id, osm_type, osm_id, class, type, name, housenumber, postcode, extratags, ST_Envelope(geometry) AS bbox, parent_place_id, linked_place_id, rank_search, importance, country_code, centroid";
  private final String selectColsOsmline = "place_id, osm_id, parent_place_id, startnumber, endnumber, interpolationtype, postcode, country_code, linegeo";
  private final String selectColsAddress = "p.place_id, p.osm_type, p.osm_id, p.name, p.class, p.type, p.rank_address, p.admin_level, p.postcode, p.extratags->'place' as place";
  private Map<String, Map<String, String>> countryNames;
  /**
   * Maps a row from location_property_osmline (address interpolation lines) to a photon doc.
   */
  private final RowMapper<NominatimResult> osmlineRowMapper = new RowMapper<NominatimResult>() {
    @Override
    public NominatimResult mapRow(final ResultSet rs, final int rownum) throws SQLException {
      final Geometry geometry = DBUtils.extractGeometry(rs, "linegeo");

      final PhotonDoc doc = new PhotonDoc(
          rs.getLong("place_id"),
          "W",
          rs.getLong("osm_id"),
          "place",
          "house_number",
          Collections.emptyMap(), // no name
          null,
          Collections.emptyMap(), // no extratags
          null,
          rs.getLong("parent_place_id"),
          0d, // importance
          CountryCode.getByCode(rs.getString("country_code")),
          null, // centroid
          0,
          30
      );
      doc.setPostcode(rs.getString("postcode"));
      doc.setCountry(getCountryNames(rs.getString("country_code")));

      final NominatimResult result = new NominatimResult(doc);
      result.addHouseNumbersFromInterpolation(rs.getLong("startnumber"), rs.getLong("endnumber"), rs.getString("interpolationtype"), geometry);

      return result;
    }
  };
  /**
   * maps a placex row in nominatim to a photon doc, some attributes are still missing and can be derived by connected address items.
   */
  private final RowMapper<NominatimResult> placeRowMapper = new RowMapper<NominatimResult>() {
    @Override
    public NominatimResult mapRow(final ResultSet rs, final int rowNum) throws SQLException {

      Double importance = rs.getDouble("importance");
      if (rs.wasNull()) {
        // https://github.com/komoot/photon/issues/12
        final int rankSearch = rs.getInt("rank_search");
        importance = 0.75 - rankSearch / 40d;
      }

      final Geometry geometry = DBUtils.extractGeometry(rs, "bbox");
      final Envelope envelope = geometry != null ? geometry.getEnvelopeInternal() : null;

      final PhotonDoc doc = new PhotonDoc(
          rs.getLong("place_id"),
          rs.getString("osm_type"),
          rs.getLong("osm_id"),
          rs.getString("class"),
          rs.getString("type"),
          DBUtils.getMap(rs, "name"),
          null,
          DBUtils.getMap(rs, "extratags"),
          envelope,
          rs.getLong("parent_place_id"),
          importance,
          CountryCode.getByCode(rs.getString("country_code")),
          DBUtils.extractGeometry(rs, "centroid"),
          rs.getLong("linked_place_id"),
          rs.getInt("rank_search")
      );

      doc.setPostcode(rs.getString("postcode"));
      doc.setCountry(getCountryNames(rs.getString("country_code")));

      final NominatimResult result = new NominatimResult(doc);
      result.addHousenumbersFromString(rs.getString("housenumber"));

      return result;
    }
  };
  private Importer importer;

  /**
   * @param host     database host
   * @param port     database port
   * @param database database name
   * @param username db username
   * @param password db username's password
   */
  public NominatimConnector(final String host, final int port, final String database, final String username, final String password) {
    final BasicDataSource dataSource = new BasicDataSource();

    dataSource.setUrl(String.format("jdbc:postgres_jts://%s:%d/%s", host, port, database));
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    dataSource.setDriverClassName(JtsWrapper.class.getCanonicalName());
    dataSource.setDefaultAutoCommit(false);

    template = new JdbcTemplate(dataSource);
    template.setFetchSize(100000);
  }

  static String convertCountryCode(final String... countryCodes) {
    String countryCodeStr = "";
    for (final String cc : countryCodes) {
      // "".split(",") results in 'new String[]{""}' and not 'new String[0]'
      if (cc.isEmpty()) {
        continue;
      }
      if (cc.length() != 2) {
        throw new IllegalArgumentException("country code invalid " + cc);
      }
      if (!countryCodeStr.isEmpty()) {
        countryCodeStr += ",";
      }
      countryCodeStr += "'" + cc.toLowerCase() + "'";
    }
    return countryCodeStr;
  }

  private Map<String, String> getCountryNames(final String countrycode) {
    if (countryNames == null) {
      countryNames = new HashMap<String, Map<String, String>>();
      template.query("SELECT country_code, name FROM country_name;", new RowCallbackHandler() {
                       @Override
                       public void processRow(final ResultSet rs) throws SQLException {
                         countryNames.put(rs.getString("country_code"), DBUtils.getMap(rs, "name"));
                       }
                     }
      );
    }

    return countryNames.get(countrycode);
  }

  public void setImporter(final Importer importer) {
    this.importer = importer;
  }

  public List<PhotonDoc> getByPlaceId(final long placeId) {
    final NominatimResult result = template.queryForObject("SELECT " + selectColsPlaceX + " FROM placex WHERE place_id = ?", new Object[] {placeId}, placeRowMapper);
    completePlace(result.getBaseDoc());
    return result.getDocsWithHousenumber();
  }

  public List<PhotonDoc> getInterpolationsByPlaceId(final long placeId) {
    final NominatimResult result = template.queryForObject("SELECT " + selectColsOsmline + " FROM location_property_osmline WHERE place_id = ?", new Object[] {placeId}, osmlineRowMapper);
    completePlace(result.getBaseDoc());
    return result.getDocsWithHousenumber();
  }

  List<AddressRow> getAddresses(final PhotonDoc doc) {
    final RowMapper<AddressRow> rowMapper = new RowMapper<AddressRow>() {
      @Override
      public AddressRow mapRow(final ResultSet rs, final int rowNum) throws SQLException {
        Integer adminLevel = rs.getInt("admin_level");
        if (rs.wasNull()) {
          adminLevel = null;
        }

        return new AddressRow(
            adminLevel,
            rs.getLong("place_id"),
            DBUtils.getMap(rs, "name"),
            rs.getString("class"),
            rs.getString("type"),
            rs.getInt("rank_address"),
            rs.getString("postcode"),
            rs.getString("place"),
            rs.getString("osm_type"),
            rs.getLong("osm_id")
        );
      }
    };

    final boolean isPoi = doc.getRankSearch() > 28;
    final long placeId = isPoi ? doc.getParentPlaceId() : doc.getPlaceId();

    final List<AddressRow> terms = template.query("SELECT " + selectColsAddress + " FROM placex p, place_addressline pa WHERE p.place_id = pa.address_place_id and pa.place_id = ? and pa.cached_rank_address > 4 and pa.address_place_id != ? and pa.isaddress order by rank_address desc,fromarea desc,distance asc,rank_search desc", new Object[] {placeId, placeId}, rowMapper);

    if (isPoi) {
      // need to add the term for the parent place ID itself
      terms.addAll(0, template.query("SELECT " + selectColsAddress + " FROM placex p WHERE p.place_id = ?", new Object[] {placeId}, rowMapper));
    }

    return terms;
  }

  /**
   * parses every relevant row in placex, creates a corresponding document and calls the {@link #importer} for every document
   */
  public void readEntireDatabase(final String... countryCodes) {
    final int progressInterval = 50000;
    final long startMillis = System.currentTimeMillis();

    String andCountryCodeStr = "", whereCountryCodeStr = "";
    final String countryCodeStr = convertCountryCode(countryCodes);
    if (!countryCodeStr.isEmpty()) {
      andCountryCodeStr = "AND country_code in (" + countryCodeStr + ")";
      whereCountryCodeStr = "WHERE country_code in (" + countryCodeStr + ")";
    }

    log.info("start importing documents from nominatim (" + (countryCodeStr.isEmpty() ? "global" : countryCodeStr) + ")");

    final BlockingQueue<PhotonDoc> documents = new LinkedBlockingDeque<>(20);
    final Thread importThread = new Thread(new ImportThread(documents));
    importThread.start();
    final AtomicLong counter = new AtomicLong();
    template.query("SELECT " + selectColsPlaceX +
                   " FROM placex " +
                   " WHERE linked_place_id IS NULL AND centroid IS NOT NULL " + andCountryCodeStr +
                   " ORDER BY geometry_sector; ", new RowCallbackHandler() {
      @Override
      public void processRow(final ResultSet rs) throws SQLException {
        // turns a placex row into a photon document that gathers all de-normalised information

        final NominatimResult docs = placeRowMapper.mapRow(rs, 0);

        if (!docs.isUsefulForIndex()) {
          return; // do not import document
        }

        // finalize document by taking into account the higher level placex rows assigned to this row
        completePlace(docs.getBaseDoc());

        for (final PhotonDoc doc : docs.getDocsWithHousenumber()) {
          while (true) {
            try {
              documents.put(doc);
            } catch (final InterruptedException e) {
              log.warn("Thread interrupted while placing document in queue.");
              continue;
            }
            break;
          }
          if (counter.incrementAndGet() % progressInterval == 0) {
            final double documentsPerSecond = 1000d * counter.longValue() / (System.currentTimeMillis() - startMillis);
            log.info(String.format("imported %s documents [%.1f/second]", MessageFormat.format("{0}", counter.longValue()), documentsPerSecond));
          }
        }
      }
    });

    template.query("SELECT place_id, osm_id, parent_place_id, startnumber, endnumber, interpolationtype, postcode, country_code, linegeo " +
                   " FROM location_property_osmline " +
                   whereCountryCodeStr +
                   " ORDER BY geometry_sector; ", new RowCallbackHandler() {
      @Override
      public void processRow(final ResultSet rs) throws SQLException {
        final NominatimResult docs = osmlineRowMapper.mapRow(rs, 0);

        if (!docs.isUsefulForIndex()) {
          return; // do not import document
        }

        // finalize document by taking into account the higher level placex rows assigned to this row
        completePlace(docs.getBaseDoc());

        for (final PhotonDoc doc : docs.getDocsWithHousenumber()) {
          while (true) {
            try {
              documents.put(doc);
            } catch (final InterruptedException e) {
              log.warn("Thread interrupted while placing document in queue.");
              continue;
            }
            break;
          }
          if (counter.incrementAndGet() % progressInterval == 0) {
            final double documentsPerSecond = 1000d * counter.longValue() / (System.currentTimeMillis() - startMillis);
            log.info(String.format("imported %s documents [%.1f/second]", MessageFormat.format("{0}", counter.longValue()), documentsPerSecond));
          }
        }
      }
    });

    while (true) {
      try {
        documents.put(FINAL_DOCUMENT);
        importThread.join();
      } catch (final InterruptedException e) {
        log.warn("Thread interrupted while placing document in queue.");
        continue;
      }
      break;
    }
    log.info(String.format("finished import of %s photon documents.", MessageFormat.format("{0}", counter.longValue())));
  }

  /**
   * retrieves a single document, used for testing / developing
   *
   * @param osmType 'N': node, 'W': way or 'R' relation
   */
  public List<PhotonDoc> readDocument(final long osmId, final char osmType) {
    return template.query("SELECT " + selectColsPlaceX + " FROM placex WHERE osm_id = ? AND osm_type = ?; ", new Object[] {osmId, osmType}, new RowMapper<PhotonDoc>() {
      @Override
      public PhotonDoc mapRow(final ResultSet resultSet, final int i) throws SQLException {
        final PhotonDoc doc = placeRowMapper.mapRow(resultSet, 0).getBaseDoc();
        completePlace(doc);
        return doc;
      }
    });
  }

  /**
   * querying nominatim's address hierarchy to complete photon doc with missing data (like country, city, street, ...)
   */
  private void completePlace(final PhotonDoc doc) {
    final List<AddressRow> addresses = getAddresses(doc);
    for (final AddressRow address : addresses) {

      if (address.hasPostcode() && doc.getPostcode() == null) {
        doc.setPostcode(address.getPostcode());
      }

      if (address.isCity()) {
        if (doc.getCity() == null) {
          doc.setCity(address.getName());
        } else {
          // there is more than one city address for this document
          if (address.hasPlace()) {
            // this city is more important than the previous one
            doc.getContext().add(doc.getCity()); // move previous city to context
            doc.setCity(address.getName()); // use new city
          } else {
            doc.getContext().add(address.getName());
          }
        }
        continue;
      }

      if (address.isCuratedCity()) {
        if (doc.getCity() == null) {
          doc.setCity(address.getName());
        } else {
          doc.getContext().add(doc.getCity()); // move previous city to context
          doc.setCity(address.getName()); // use new city
        }
        // do not continue as a curated city might be a state as well
      }

      if (address.isStreet() && doc.getStreet() == null) {
        doc.setStreet(address.getName());
        continue;
      }

      if (address.isState() && doc.getState() == null) {
        doc.setState(address.getName());
        continue;
      }

      // no specifically handled item, check if useful for context
      if (address.isUsefulForContext()) {
        doc.getContext().add(address.getName());
      }
    }
  }

  private class ImportThread implements Runnable {

    private final BlockingQueue<PhotonDoc> documents;

    public ImportThread(final BlockingQueue<PhotonDoc> documents) {
      this.documents = documents;
    }

    @Override
    public void run() {
      while (true) {
        final PhotonDoc doc;
        try {
          doc = documents.take();
          if (doc == FINAL_DOCUMENT) {
            break;
          }
          importer.add(doc);
        } catch (final InterruptedException e) {
          log.info("interrupted exception ", e);
        }
      }
      importer.finish();
    }
  }
}
