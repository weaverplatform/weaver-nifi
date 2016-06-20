package com.weaverplatform.nifi.individual;

import com.weaverplatform.sdk.*;
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
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, create, individualproperty"})
@CapabilityDescription("Creates an individual property")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class CreateIndividualProperty extends FlowFileProcessor {

  public static final PropertyDescriptor SUBJECT_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Subject Attribute")
    .description("Look for the FlowFile attribute.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor SUBJECT_STATIC = new PropertyDescriptor
    .Builder().name("Subject Static")
    .description("If there is no FlowFile attribute, use static value.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor PREDICATE_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Predicate Attribute")
    .description("Look for the FlowFile attribute.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor PREDICATE_STATIC = new PropertyDescriptor
    .Builder().name("Predicate Static")
    .description("If there is no FlowFile attribute, use static value.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor OBJECT_ATTRIBUTE = new PropertyDescriptor
    .Builder().name("Object Attribute")
    .description("Look for the FlowFile attribute.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor OBJECT_STATIC = new PropertyDescriptor
    .Builder().name("Object Static")
    .description("If there is no FlowFile attribute, use static value.")
    .required(false)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();

  public static final PropertyDescriptor IS_ADDIFYING = new PropertyDescriptor
      .Builder().name("Is Addifying?")
      .description("If this attribute is set, the object or subject entity will be " +
          "created if it does not already exist. (leave this field empty to disallow " +
          "this behaviour)")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();

  @Override
  protected void init(final ProcessorInitializationContext context) {

    super.init(context);

    descriptors.add(SUBJECT_ATTRIBUTE);
    descriptors.add(SUBJECT_STATIC);
    descriptors.add(PREDICATE_ATTRIBUTE);
    descriptors.add(PREDICATE_STATIC);
    descriptors.add(OBJECT_ATTRIBUTE);
    descriptors.add(OBJECT_STATIC);
    descriptors.add(IS_ADDIFYING);
    this.properties = Collections.unmodifiableList(descriptors);


    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    final ProcessorLog log = this.getLogger();

    super.onTrigger(context, session);
    Weaver weaver = getWeaver();

    String subjectId = valueFromOptions(context, flowFile, SUBJECT_ATTRIBUTE, SUBJECT_STATIC, null);
    String predicate = valueFromOptions(context, flowFile, PREDICATE_ATTRIBUTE, PREDICATE_STATIC, null);
    String objectId = valueFromOptions(context, flowFile, OBJECT_ATTRIBUTE, OBJECT_STATIC, null);
    String id = idFromOptions(context, flowFile, true);

    log.info("Creating Individual Property with SUBJECT: " + subjectId + ", OBJECT: " + objectId +  ", and PREDICATE: " + predicate + ".");

    // Get the parent object from weaver
    Entity subjectEntity = weaver.get(subjectId);

    if (subjectId != null && objectId != null) {

      if (context.getProperty(IS_ADDIFYING).isSet() &&
          !EntityType.INDIVIDUAL.equals(subjectEntity.getType())) {

        //Create an empty parent object if one does not already exist
        log.info("Subject entity could not be found in Weaver. Creating..");

        Map<String, Object> attributes = new HashMap<>();
        subjectEntity = createIndividual(subjectId, attributes);

      }

      // Create child attributes
      Map<String, Object> entityAttributes = new HashMap<>();
      entityAttributes.put("predicate", predicate);

      // Find the object
      Entity objectEntity = weaver.get(objectId);

      if (context.getProperty(IS_ADDIFYING).isSet() &&
          !EntityType.INDIVIDUAL.equals(objectEntity.getType())) {
        log.info("Creating temporary entity..");

        Map<String, Object> attributes = new HashMap<>();
        objectEntity = createIndividual(objectId, attributes);

      }

      Map<String, ShallowEntity> relations = new HashMap<>();
      relations.put(RelationKeys.SUBJECT, subjectEntity);
      relations.put(RelationKeys.OBJECT, objectEntity);

      Entity individualProperty = weaver.add(entityAttributes, EntityType.INDIVIDUAL_PROPERTY, id, relations);

      // Fetch parent collection
      ShallowEntity shallowCollection = subjectEntity.getRelations().get(RelationKeys.PROPERTIES);
      Entity entityProperties = weaver.get(shallowCollection.getId());

      // Link individual to collection
      entityProperties.linkEntity(individualProperty.getId(), individualProperty);

    } else {

      log.warn("Attempted to create Individual property without supplying a subject and object");

    }

//    weaver.close();

    if (context.getProperty(ATTRIBUTE_NAME_FOR_ID).isSet()) {
      String attributeNameForId = context.getProperty(ATTRIBUTE_NAME_FOR_ID).getValue();
      flowFile = session.putAttribute(flowFile, attributeNameForId, id);
    }
    session.transfer(flowFile, ORIGINAL);

  }

  @Override
  protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {

    return new PropertyDescriptor.Builder()
      .required(false)
      .name(propertyDescriptorName)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .dynamic(true)
      .expressionLanguageSupported(false)
      .build();
  }

  private Entity createIndividual(String id, Map<String, Object> attributes) {
    Weaver weaver = getWeaver();

    Entity individual = weaver.add(attributes, EntityType.INDIVIDUAL, id);

    Entity entityProperties = weaver.collection();
    individual.linkEntity(RelationKeys.PROPERTIES, entityProperties);

    Entity entityAnnotations = weaver.collection();
    individual.linkEntity(RelationKeys.ANNOTATIONS, entityAnnotations);

    return individual;
  }
}