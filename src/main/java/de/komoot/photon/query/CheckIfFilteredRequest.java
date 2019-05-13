package de.komoot.photon.query;

import de.komoot.photon.utils.Command;

import spark.QueryParamsMap;

/**
 * Created by Sachin Dole on 2/22/2015.
 */
public class CheckIfFilteredRequest implements Command<Boolean, QueryParamsMap> {

  @Override
  public Boolean execute(final QueryParamsMap... operand) {
    final QueryParamsMap queryParam = operand[0];
    if (!queryParam.hasValue()) {
      return false;
    }
    final String[] tagsToFilterOn = queryParam.values();
    for (final String eachTagToFilterOn : tagsToFilterOn) {
      if (eachTagToFilterOn != null && eachTagToFilterOn.trim().length() > 0) {
        return true;
      }
    }
    return false;
  }
}
