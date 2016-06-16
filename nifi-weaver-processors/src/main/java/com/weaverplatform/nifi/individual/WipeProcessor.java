package com.weaverplatform.nifi.individual;

import com.weaverplatform.nifi.WeaverProcessor;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
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

@Tags({"weaver, wipe"})
@CapabilityDescription("Wipe whole weaver")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class WipeProcessor extends WeaverProcessor {

  FlowFile flowFile;


  public static final Relationship ORIGINAL = new Relationship.Builder()
      .name("Original Content")
      .description("Relationship to send original content to to.")
      .build();
  


  @Override
  protected void init(final ProcessorInitializationContext context) {
    
    super.init(context); 

    this.properties = Collections.unmodifiableList(descriptors);

    relationshipSet.add(ORIGINAL);
    this.relationships = new AtomicReference<>(relationshipSet);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    super.onTrigger(context, session);

    final ProcessorLog log = this.getLogger();
    flowFile = session.get();
    weaver.wipe();
    session.transfer(flowFile, ORIGINAL);
  }
}
