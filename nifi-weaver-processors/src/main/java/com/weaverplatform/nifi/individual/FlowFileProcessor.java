package com.weaverplatform.nifi.individual;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

/**
 * @author Bastiaan Bijl
 */
public abstract class FlowFileProcessor extends EntityProcessor {

  FlowFile flowFile;
  
  public static final Relationship ORIGINAL = new Relationship.Builder()
      .name("Original Content")
      .description("Relationship to send original content to to.")
      .build();

  public FlowFile getFlowFile() {
    return flowFile;
  }

  @Override
  protected void init(final ProcessorInitializationContext context) {

    super.init(context);

    relationshipSet.add(ORIGINAL);
  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    super.onTrigger(context, session);

    flowFile = session.get();
    if (flowFile == null) {
      throw new RuntimeException("FlowFile is null");
    }

  }
}