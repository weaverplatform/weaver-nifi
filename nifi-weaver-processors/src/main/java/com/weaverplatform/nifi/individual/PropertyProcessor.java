package com.weaverplatform.nifi.individual;

import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.ShallowEntity;
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.json.request.ReadPayload;

/**
 * @author Mohamad Alamili
 */
public abstract class PropertyProcessor extends FlowFileProcessor {

  protected Entity getProperty(Weaver weaver, Entity subject, String predicate, String source){
    ShallowEntity relationsShallow = subject.getRelations().get("properties");

    if (relationsShallow == null){
      return null;
    }

    // Load relations and check for existence
    Entity relations = weaver.get(relationsShallow.getId(), new ReadPayload.Opts(1));
    for(ShallowEntity shallowRelation : relations.getRelations().values()){
      Entity relation = weaver.get(shallowRelation.getId(), new ReadPayload.Opts(1));

      String foundPredicate = relation.getAttributes().get("predicate");
      String foundSource    = relation.getAttributes().get("source");

      if(predicate.equals(foundPredicate) && source.equals(foundSource)){
        return relation;
      }
    }

    // Not found
    return null;
  }
}