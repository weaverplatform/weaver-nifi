package com.weaverplatform.nifi;

import com.weaverplatform.nifi.util.WeaverProperties;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.NiFiProperties;

/**
 * @author Bastiaan Bijl
 */
public abstract class WeaverProcessor extends AbstractProcessor {

  public String weaverUrl;

  public static final PropertyDescriptor WEAVER = new PropertyDescriptor
      .Builder().name("Weaver URL")
      .description("weaver connection url i.e. weaver.connect(url)")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();



  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    // Weaver URL
    if(context.getProperty(WEAVER).getValue() != null) {
      weaverUrl = context.getProperty(WEAVER).getValue();
    }
    else {
      weaverUrl = NiFiProperties.getInstance().get(WeaverProperties.URL).toString();
    }
  }
}
