package com.weaverplatform.nifi.individual;

import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.EntityNotFoundException;
import com.weaverplatform.sdk.ShallowEntity;
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.json.request.ReadPayload;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, property, exists"})
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class PropertyExists extends EntityProcessor {
  
  public static final Relationship EXISTS = new Relationship.Builder()
    .name("Exists")
    .description("Original FlowFile if property exists.")
    .build();
  
  public static final Relationship NOT_EXISTS = new Relationship.Builder()
    .name("Not exists")
    .description("Original FlowFile if property does not exist.")
    .build();

  public static final PropertyDescriptor PREDICATE_STATIC = new PropertyDescriptor
    .Builder().name("Predicate Static")
    .description("Static predicate.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor PREDICATE_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Predicate Attribute")
    .description("Look for a FlowFile attribute to set the predicate.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  @Override
  protected void init(final ProcessorInitializationContext context) {
    
    super.init(context);

    descriptors.add(PREDICATE_STATIC);
    descriptors.add(PREDICATE_ATTRIBUTE);
    this.properties = Collections.unmodifiableList(descriptors);

    relationshipSet.add(EXISTS);
    relationshipSet.add(NOT_EXISTS);
    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    super.onTrigger(context, session);
    Weaver weaver = getWeaver();

    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }
    
    // Get the predicate
    String predicate = valueFromOptions(context, flowFile, PREDICATE_ATTRIBUTE, PREDICATE_STATIC, null);
    if(predicate == null){
      throw new ProcessException("Neither Predicate Attribute nor Predicate Static is set");
    }
    
    // Load entity
    String id = idFromOptions(context, flowFile, false);
    Entity entity;
    try {
      entity = weaver.get(id, new ReadPayload.Opts(1));
    }
    catch(EntityNotFoundException e) {
      throw new ProcessException("Individual does not exists");
    }
    
    ShallowEntity relationsShallow = entity.getRelations().get("properties");
    
    if (relationsShallow == null){
      session.transfer(flowFile, NOT_EXISTS);
      return;
    }
    
    // Load relations and check for existence
    Entity relations = weaver.get(relationsShallow.getId(), new ReadPayload.Opts(1));
    for(ShallowEntity shallowRelation : relations.getRelations().values()){
      Entity relation = weaver.get(shallowRelation.getId(), new ReadPayload.Opts(1));
      if(predicate.equals(relation.getAttributes().get("predicate"))){
        session.transfer(flowFile, EXISTS);
        return;
      }
    }

    // Nothing found
    session.transfer(flowFile, NOT_EXISTS);
  }
}