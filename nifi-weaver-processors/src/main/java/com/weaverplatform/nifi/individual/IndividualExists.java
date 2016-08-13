package com.weaverplatform.nifi.individual;

import com.weaverplatform.sdk.EntityNotFoundException;
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.json.request.ReadPayload;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, individual, exists, deprecated"})
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class IndividualExists extends EntityProcessor {
  
  public static final Relationship EXISTS = new Relationship.Builder()
    .name("Exists")
    .description("Original FlowFile if individual exists.")
    .build();
  
  public static final Relationship NOT_EXISTS = new Relationship.Builder()
    .name("Not exists")
    .description("Original FlowFile if individual does not exist.")
    .build();

  private volatile Set<String> dynamicPropertyNames;
  private Map<String, PropertyValue> propertyMap;

  @Override
  protected void init(final ProcessorInitializationContext context) {
    
    super.init(context);

    this.properties = Collections.unmodifiableList(descriptors);

    relationshipSet.add(EXISTS);
    relationshipSet.add(NOT_EXISTS);
    this.relationships = new AtomicReference<>(relationshipSet);

    // For dynamic properties
    this.dynamicPropertyNames = new HashSet<>();
    this.propertyMap = new HashMap<>();
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

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    final ProcessorLog log = this.getLogger();

    Weaver weaver = getWeaver();

    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }
    
    String id = idFromOptions(context, flowFile, false);
    try {
      weaver.get(id, new ReadPayload.Opts(0));
      session.transfer(flowFile, EXISTS);
    } catch (EntityNotFoundException e) {
      session.transfer(flowFile, NOT_EXISTS);
    }
  }
}