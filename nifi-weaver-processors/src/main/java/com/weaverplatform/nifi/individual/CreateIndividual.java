package com.weaverplatform.nifi.individual;

import com.weaverplatform.sdk.*;
import com.weaverplatform.sdk.json.request.UpdateEntityAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.components.PropertyValue;
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

  public static final PropertyDescriptor NAME_PREFIX = new PropertyDescriptor
    .Builder().name("Name Prefix")
    .description("If this is set all names are prefixed with this string (add a trailing space yourself).")
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

  private Entity individual;
  private Entity entityProperties;
  private Entity entityAnnotations;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    
    super.init(context); 
    
    
    descriptors.add(NAME_ATTRIBUTE);
    descriptors.add(NAME_STATIC);
    descriptors.add(NAME_PREDICATE_STATIC);
    descriptors.add(NAME_PREFIX);
    descriptors.add(IS_ADDIFYING);
    this.properties = Collections.unmodifiableList(descriptors);


    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    final ProcessorLog log = this.getLogger();
    
    super.onTrigger(context, session);

    String id = idFromOptions(context, flowFile, true);
    String name = getName(context);
    PropertyValue predicateProperty = context.getProperty(NAME_PREDICATE_STATIC);
    String predicate = context.getProperty(NAME_PREDICATE_STATIC).getValue();

    // Should we be prepared for the possibility that this entity has already been created.
    boolean isAddifying = context.getProperty(IS_ADDIFYING).isSet();

    // Create without checking for entities prior existence
    if(!isAddifying) {

      Map<String, Object> attributes = new HashMap<>();
      attributes.put("name", name);

      createIndividual(id, attributes);

      // If a name predicate is set, create a name predicate and a name property
      if(predicateProperty.isSet()) {
        createNameValueProperty(predicate, name);
      }

      // Attach to dataset
      datasetObjects.linkEntity(id, individual);

    // Check to see whether it exists before creation
    } else {
      individual = weaver.get(id);

      // It already exists, check it's attributes.
      if(EntityType.INDIVIDUAL.equals(individual.getType())) {

        // Check if name attribute is set
        if(!individual.getAttributes().containsKey("name")){
          weaver.updateEntityAttribute(new UpdateEntityAttribute(individual.getId(), "name", name));

        }
        if(!name.equals(individual.getAttributes().get("name"))) {
          individual.updateEntityWithValue("name", name);
        }

        // Check if a name property needs to be added
        boolean predicateIsSet = false;
        if(predicateProperty.isSet()) {
          for(ShallowEntity shallowEntityProperty : entityProperties.getRelations().values()) {
            Entity entityProperty = weaver.get(shallowEntityProperty.getId());
            try {
              if(entityProperty.getAttributeValue("predicate").equals(predicate)) {

                if(!entityProperty.getAttributeValue("object").equals(name)) {
                  entityProperty.updateEntityWithValue("object", name);
                }

                predicateIsSet = true;
                break;
              }
            } catch (EntityAttributeNotFoundException e) {}
          }

          if(!predicateIsSet) {
            createNameValueProperty(predicate, name);
          }
        }

        // Check if it is attached to a dataset
        if(!datasetObjects.getRelations().keySet().contains(individual.getId())) {
          datasetObjects.linkEntity(id, individual);
        }

      // It does not exist yet
      } else {
        
        Map<String, Object> attributes = new HashMap<>();
        attributes.put("name", name);

        createIndividual(id, attributes);

        // If a name predicate is set, create a name predicate and a name property
        if(predicateProperty.isSet()) {
          createNameValueProperty(predicate, name);
        }

        // Attach to dataset
        datasetObjects.linkEntity(id, individual);

      }

    }

//    weaver.close();

    if(context.getProperty(ATTRIBUTE_NAME_FOR_ID).isSet()) {
      String attributeNameForId = context.getProperty(ATTRIBUTE_NAME_FOR_ID).getValue();
      flowFile = session.putAttribute(flowFile, attributeNameForId, id);
    }
    session.transfer(flowFile, ORIGINAL);
  }

  private void createNameValueProperty(String predicate, String name) {

    // Make name annotation
    HashMap<String, Object> nameAnnotationAttributes = new HashMap<>();
    nameAnnotationAttributes.put("label", predicate);
    nameAnnotationAttributes.put("celltype", "string");
    nameAnnotationAttributes.put("datatype", "xsd:string");
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

  private String getName(ProcessContext context) {
    String name = valueFromOptions(context, flowFile, NAME_ATTRIBUTE, NAME_STATIC, "Unnamed");

    // Check for prefix
    if(context.getProperty(NAME_PREFIX).isSet()) {
      name = context.getProperty(NAME_PREFIX).getValue() + name;
    }
    return name;
  }

  private void createIndividual(String id, Map<String, Object> attributes) {

    individual = weaver.add(attributes, EntityType.INDIVIDUAL, id);

    entityProperties = weaver.collection();
    individual.linkEntity(RelationKeys.PROPERTIES, entityProperties);

    entityAnnotations = weaver.collection();
    individual.linkEntity(RelationKeys.ANNOTATIONS, entityAnnotations);
  }
}
