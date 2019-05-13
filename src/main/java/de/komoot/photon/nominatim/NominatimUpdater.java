package de.komoot.photon.nominatim;

import de.komoot.photon.PhotonDoc;
import de.komoot.photon.Updater;
import de.komoot.photon.nominatim.model.UpdateRow;

import org.apache.commons.dbcp.BasicDataSource;
import org.postgis.jts.JtsWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Nominatim update logic
 *
 * @author felix
 */

public class NominatimUpdater {

  private static final Logger LOGGER = LoggerFactory.getLogger(NominatimUpdater.class);

  private static final int CREATE = 1;
  private static final int UPDATE = 2;
  private static final int DELETE = 100;

  private static final int MIN_RANK = 1;
  private static final int MAX_RANK = 30;

  private final JdbcTemplate template;
  private final NominatimConnector exporter;

  private Updater updater;

  /**
   * when updating lockout other threads
   */
  private final ReentrantLock updateLock = new ReentrantLock();

  /**
   * Creates a new instance
   *
   * @param host     Nominatim database host
   * @param port     Nominatim database port
   * @param database Nominatim database name
   * @param username Nominatim database username
   * @param password Nominatim database password
   */
  public NominatimUpdater(final String host, final int port, final String database, final String username, final String password) {
    final BasicDataSource dataSource = new BasicDataSource();

    dataSource.setUrl(String.format("jdbc:postgresql://%s:%d/%s", host, port, database));
    dataSource.setUsername(username);
    dataSource.setPassword(password);
    dataSource.setDriverClassName(JtsWrapper.class.getCanonicalName());
    dataSource.setDefaultAutoCommit(true);

    exporter = new NominatimConnector(host, port, database, username, password);
    template = new JdbcTemplate(dataSource);
  }

  public void setUpdater(final Updater updater) {
    this.updater = updater;
  }

  public void update() {
    if (updateLock.tryLock()) {
      try {
        int updatedPlaces = 0;
        int deletedPlaces = 0;
        for (int rank = MIN_RANK; rank <= MAX_RANK; rank++) {
          LOGGER.info(String.format("Starting rank %d", rank));
          for (final Map<String, Object> sector : getIndexSectors(rank)) {
            for (final UpdateRow place : getIndexSectorPlaces(rank, (Integer) sector.get("geometry_sector"))) {
              final long placeId = place.getPlaceId();
              template.update("update placex set indexed_status = 0 where place_id = ?;", placeId);

              Integer indexedStatus = place.getIndexdStatus();
              if (indexedStatus == DELETE || indexedStatus == UPDATE && rank == MAX_RANK) {
                updater.delete(placeId);
                if (indexedStatus == DELETE) {
                  deletedPlaces++;
                  continue;
                }
                indexedStatus = CREATE; // always create
              }
              updatedPlaces++;

              final List<PhotonDoc> updatedDocs = exporter.getByPlaceId(place.getPlaceId());
              boolean wasUseful = false;
              for (final PhotonDoc updatedDoc : updatedDocs) {
                switch (indexedStatus) {
                  case CREATE:
                    if (updatedDoc.isUsefulForIndex()) {
                      updater.create(updatedDoc);
                    }
                    break;
                  case UPDATE:
                    if (updatedDoc.isUsefulForIndex()) {
                      updater.updateOrCreate(updatedDoc);
                      wasUseful = true;
                    }
                    break;
                  default:
                    LOGGER.error(String.format("Unknown index status %d", indexedStatus));
                    break;
                }
              }
              if (indexedStatus == UPDATE && !wasUseful) {
                // only true when rank != 30
                // if no documents for the place id exist this will likely cause moaning
                updater.delete(placeId);
                updatedPlaces--;
              }
            }
          }
        }

        LOGGER.info(String.format("%d places created or updated, %d deleted", updatedPlaces, deletedPlaces));

        // update documents generated from address interpolations
        // .isUsefulForIndex() should always return true for documents
        // created from interpolations so no need to check them
        LOGGER.info("Starting interpolations");
        int updatedInterpolations = 0;
        int deletedInterpolations = 0;
        int interpolationDocuments = 0;
        for (final Map<String, Object> sector : template.queryForList(
            "select geometry_sector,count(*) from location_property_osmline where indexed_status > 0 group by geometry_sector order by geometry_sector;")) {
          for (final UpdateRow place : getIndexSectorInterpolations((Integer) sector.get("geometry_sector"))) {
            final long placeId = place.getPlaceId();
            template.update("update location_property_osmline set indexed_status = 0 where place_id = ?;", placeId);

            final Integer indexedStatus = place.getIndexdStatus();
            if (indexedStatus != CREATE) {
              updater.delete(placeId);
              if (indexedStatus == DELETE) {
                deletedInterpolations++;
                continue;
              }
            }
            updatedInterpolations++;

            final List<PhotonDoc> updatedDocs = exporter.getInterpolationsByPlaceId(place.getPlaceId());
            for (final PhotonDoc updatedDoc : updatedDocs) {
              updater.create(updatedDoc);
              interpolationDocuments++;
            }
          }
        }
        LOGGER.info(String.format("%d interpolations created or updated, %d deleted, %d documents added or updated", updatedInterpolations,
                                  deletedInterpolations, interpolationDocuments));
        updater.finish();
        template.update("update import_status set indexed=true;"); // indicate that we are finished

        LOGGER.info("Finished updating");
      } finally {
        updateLock.unlock();
      }
    } else {
      LOGGER.info("Update already in progress");
    }
  }

  private List<Map<String, Object>> getIndexSectors(final Integer rank) {
    return template.queryForList("select geometry_sector,count(*) from placex where rank_search = ? "
                                 + "and indexed_status > 0 group by geometry_sector order by geometry_sector;", rank);
  }

  private List<UpdateRow> getIndexSectorPlaces(final Integer rank, final Integer geometrySector) {
    return template.query("select place_id, indexed_status from placex where rank_search = ?" + " and geometry_sector = ? and indexed_status > 0;",
                          new Object[] {rank, geometrySector}, new RowMapper<UpdateRow>() {
          @Override
          public UpdateRow mapRow(final ResultSet rs, final int rowNum) throws SQLException {
            final UpdateRow updateRow = new UpdateRow();
            updateRow.setPlaceId(rs.getLong("place_id"));
            updateRow.setIndexdStatus(rs.getInt("indexed_status"));
            return updateRow;
          }
        });
  }

  private List<UpdateRow> getIndexSectorInterpolations(final Integer geometrySector) {
    return template.query("select place_id, indexed_status from location_property_osmline where geometry_sector = ? and indexed_status > 0;",
                          new Object[] {geometrySector}, new RowMapper<UpdateRow>() {
          @Override
          public UpdateRow mapRow(final ResultSet rs, final int rowNum) throws SQLException {
            final UpdateRow updateRow = new UpdateRow();
            updateRow.setPlaceId(rs.getLong("place_id"));
            updateRow.setIndexdStatus(rs.getInt("indexed_status"));
            return updateRow;
          }
        });
  }
}
