package com.weaverplatform.nifi.individual;

import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.EntityType;
import com.weaverplatform.sdk.RelationKeys;
import com.weaverplatform.sdk.ShallowEntity;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, create, individual"})
@CapabilityDescription("Create individual object")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CreateIndividual extends DatasetProcessor {
  
  public static final PropertyDescriptor NAME_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Name Attribute")
    .description("Look for a FlowFile attribute to set the name.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor NAME_STATIC = new PropertyDescriptor
    .Builder().name("Name Static")
    .description("Look for a FlowFile attribute to set the name.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor NAME_PREDICATE_STATIC = new PropertyDescriptor
    .Builder().name("Name Predicate Static")
    .description("Use this predicate to set the name as ValueProperty.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  @Override
  protected void init(final ProcessorInitializationContext context) {
    
    super.init(context); 
    
    
    descriptors.add(NAME_ATTRIBUTE);
    descriptors.add(NAME_STATIC);
    descriptors.add(NAME_PREDICATE_STATIC);
    this.properties = Collections.unmodifiableList(descriptors);


    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    final ProcessorLog log = this.getLogger();
    
    super.onTrigger(context, session);

    String id = idFromOptions(context, flowFile, true);

    // Create entity by user attribute
    Map<String, Object> attributes = new HashMap<>();
    String name = null;
    String nameAttribute = context.getProperty(NAME_ATTRIBUTE).getValue();
    if(nameAttribute != null) {
      name = flowFile.getAttribute(nameAttribute);
    }
    if(name == null) {
      name = "Unnamed";
    }
    attributes.put("name", name);
    log.info("create individual with name " + name);
    
    log.info("create individual with id "+id);
    Entity individual = weaver.add(attributes, EntityType.INDIVIDUAL, id);
    
    // Attach to dataset
    datasetObjects.linkEntity(id, individual);

    Entity entityProperties = weaver.collection();
    individual.linkEntity(RelationKeys.PROPERTIES, entityProperties);
    
    
    Entity entityAnnotations = weaver.collection();
    individual.linkEntity(RelationKeys.ANNOTATIONS, entityAnnotations);
    
    // If a name predicate is set, create a name predicate and a name property
    if(context.getProperty(NAME_PREDICATE_STATIC) != null) {
      
      String predicate = context.getProperty(NAME_PREDICATE_STATIC).getValue();

      // Make name annotation
      HashMap<String, Object> nameAnnotationAttributes = new HashMap<>();
      nameAnnotationAttributes.put("label", "rdfs:label");
      nameAnnotationAttributes.put("celltype", "string");
      Entity nameAnnotation = weaver.add(nameAnnotationAttributes, EntityType.ANNOTATION);
      entityAnnotations.linkEntity(nameAnnotation.getId(), nameAnnotation);

      // Make a name property
      Map<String, ShallowEntity> relations = new HashMap<>();
      relations.put("subject", individual);
      relations.put("annotation", nameAnnotation);

      HashMap<String, Object> propertyAttributes = new HashMap<>();
      propertyAttributes.put("predicate", predicate);
      propertyAttributes.put("object", name);

      Entity nameProperty = weaver.add(propertyAttributes, EntityType.VALUE_PROPERTY, UUID.randomUUID().toString(), relations);
      entityProperties.linkEntity(nameProperty.getId(), nameProperty);
      
    }
    
    weaver.close();

    try {
      String attributeNameForId = context.getProperty(ATTRIBUTE_NAME_FOR_ID).getValue();
      flowFile = session.putAttribute(flowFile, attributeNameForId, id);
    } catch(Exception e){
      log.error(e.getMessage() + "fired!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!GGGG");
    }
    session.transfer(flowFile, ORIGINAL);
  }
}
