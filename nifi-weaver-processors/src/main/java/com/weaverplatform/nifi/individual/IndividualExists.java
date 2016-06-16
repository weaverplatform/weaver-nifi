package com.weaverplatform.nifi.individual;

import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.EntityType;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"weaver, individual, exists"})
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

  @Override
  protected void init(final ProcessorInitializationContext context) {
    
    super.init(context);

    this.properties = Collections.unmodifiableList(descriptors);

    relationshipSet.add(EXISTS);
    relationshipSet.add(NOT_EXISTS);
    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    final ProcessorLog log = this.getLogger();

    super.onTrigger(context, session);

    FlowFile flowFile = session.get();
    if (flowFile == null) {
      return;
    }
    
    String id = idFromOptions(context, flowFile, false);
    Entity entity = weaver.get(id);

    if(EntityType.INDIVIDUAL.equals(entity.getType())) {
      session.transfer(flowFile, EXISTS);
    } else {
      session.transfer(flowFile, NOT_EXISTS);
    }

//    weaver.close();
  }
}