package com.weaverplatform.nifi.individual;

import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.ShallowEntity;
import com.weaverplatform.sdk.json.request.QueryFromFilter;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, get, property"})
@CapabilityDescription("Find an entity id")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class GetIdFromProperty extends FlowFileProcessor {

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



  @Override
  protected void init(final ProcessorInitializationContext context) {

    super.init(context);

    descriptors.add(SUBJECT_ATTRIBUTE);
    descriptors.add(SUBJECT_STATIC);
    descriptors.add(PREDICATE_ATTRIBUTE);
    descriptors.add(PREDICATE_STATIC);
    descriptors.add(OBJECT_ATTRIBUTE);
    descriptors.add(OBJECT_STATIC);
    this.properties = Collections.unmodifiableList(descriptors);


    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    final ProcessorLog log = this.getLogger();

    super.onTrigger(context, session);

    String attributeNameForId = context.getProperty(ATTRIBUTE_NAME_FOR_ID).getValue();

    String subject = valueFromOptions(context, flowFile, SUBJECT_ATTRIBUTE, SUBJECT_STATIC, null);
    String predicate = valueFromOptions(context, flowFile, PREDICATE_ATTRIBUTE, PREDICATE_STATIC, null);
    String object = valueFromOptions(context, flowFile, OBJECT_ATTRIBUTE, OBJECT_STATIC, null);
    
    if(predicate == null) {
      throw new ProcessException("Predicate not set for GetIdFromProperty.");
    }
    if(subject == null && object == null) {
      throw new ProcessException("GetIdFromProperty should be able to find subject or object. It did not find both.");
    }

    
    if(object == null) {
      try {

        // Get the subject from weaver
        Entity individual = weaver.get(subject);
        Map<String, ShallowEntity> relations = individual.getRelations();
        for(ShallowEntity relationShell : relations.values()) {
          
          Entity relation = weaver.get(relationShell.getId());
          if(predicate.equals(relation.getAttributeValue("predicate"))) {
            log.info("found object");
            Object relationshipObject = relation.getAttributeValue("object");
            if(relationshipObject instanceof Entity) {
              log.info("actually an entity");
              sendFoundId(session, attributeNameForId, ((Entity)relationshipObject).getId());
            } else
            if(relationshipObject instanceof ShallowEntity) {
              log.info("actually a shallowentity");
              sendFoundId(session, attributeNameForId, ((ShallowEntity)relationshipObject).getId());
            } else {
              log.info("skipping, was a string and not an entity (GetIdFromProperty)");
            }
          }
          
        }
        if(!relations.containsKey(predicate)) {
          throw new ProcessException("GetIdFromProperty found the subject "+subject+", but it did not have the predicate "+predicate+".");
        }

      } catch (IndexOutOfBoundsException e) {
        throw new ProcessException(e);
      } catch (NullPointerException e) {
        throw new ProcessException(e);
      }
    } else if(subject == null) {
      try {

        // Get the object from weaver
        Entity individual = weaver.get(object);

        ArrayList<QueryFromFilter> filters = new ArrayList<>();
        QueryFromFilter filter = new QueryFromFilter(predicate);
        filter.addIndividualCondition("this-individual", individual.getId());
        filters.add(filter);
        ArrayList<String> results = weaver.queryFromFilters(filters);

        for(String subjectId : results) {
          sendFoundId(session, attributeNameForId, subjectId);
        }

      } catch (IndexOutOfBoundsException e) {
        throw new ProcessException(e);
      } catch (NullPointerException e) {
        throw new ProcessException(e);
      }
    } else {
      throw new ProcessException("Either subject or object should be empty for GetIdeFromProperty.");
    }

    weaver.close();
  }
  
  private void sendFoundId(ProcessSession session, String attributeName, String id) {
    FlowFile clonedFlowFile = session.clone(flowFile);
    clonedFlowFile = session.putAttribute(clonedFlowFile, attributeName, id);
    session.transfer(clonedFlowFile, ORIGINAL);
  }


}