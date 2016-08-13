package com.weaverplatform.nifi.individual;

import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.ShallowEntity;
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.json.request.ReadPayload;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Mohamad Alamili
 */
public abstract class PropertyProcessor extends FlowFileProcessor {

  /**
   * The key of the map is the ID of the object or the value of the object
   * @param weaver
   * @param subject
   * @param predicate
   * @return
   */
  protected Map<String, Entity> getProperty(Weaver weaver, Entity subject, String predicate) {
    Map<String, Entity> foundProperties = new HashMap<>();
    
    ShallowEntity relationsShallow = subject.getRelations().get("properties");

    if (relationsShallow == null){
      return null;
    }

    // Load relations and check for existence
    Entity relations = weaver.get(relationsShallow.getId(), new ReadPayload.Opts(1));

    for(ShallowEntity shallowRelation : relations.getRelations().values()){
      Entity relation = weaver.get(shallowRelation.getId(), new ReadPayload.Opts(1));

      String foundPredicate = relation.getRelations().get("predicate").getId();

      if(predicate.equals(foundPredicate)){
        if (relation.getType().equals("$INDIVIDUAL_PROPERTY"))
          foundProperties.put(relation.getRelations().get("object").getId(), relation);
        else
          foundProperties.put(relation.getAttributes().get("object"), relation);
      }
    }

    // Not found
    if (foundProperties.isEmpty()){
      return null;
    }
    else
      return foundProperties;
  }
}