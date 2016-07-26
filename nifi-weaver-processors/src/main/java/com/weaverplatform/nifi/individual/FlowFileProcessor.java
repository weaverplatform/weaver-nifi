package com.weaverplatform.nifi.individual;

import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;

/**
 * @author Bastiaan Bijl
 */
public abstract class FlowFileProcessor extends EntityProcessor {
  
  public static final Relationship ORIGINAL = new Relationship.Builder()
      .name("Original Content")
      .description("Relationship to send original content to to.")
      .build();

  @Override
  protected void init(final ProcessorInitializationContext context) {

    super.init(context);

    relationshipSet.add(ORIGINAL);
  }
}