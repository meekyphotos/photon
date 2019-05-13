package de.komoot.photon;

/**
 * @author felix
 */
public interface Updater {

  void create(PhotonDoc doc);

  void update(PhotonDoc doc);

  void delete(Long id);

  void finish();

  void updateOrCreate(PhotonDoc updatedDoc);
}
