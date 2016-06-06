package com.weaverplatform.nifi;

import com.weaverplatform.nifi.util.WeaverProperties;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.NiFiProperties;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Bastiaan Bijl
 */
public abstract class WeaverProcessor extends AbstractProcessor {



  public final List<PropertyDescriptor> descriptors = new ArrayList<>();
  public List<PropertyDescriptor> properties;
  
  public final Set<Relationship> relationshipSet = new HashSet<>();
  public AtomicReference<Set<Relationship>> relationships;

  public String weaverUrl;

  public static final PropertyDescriptor WEAVER = new PropertyDescriptor
      .Builder().name("Weaver URL")
      .description("weaver connection url i.e. weaver.connect(url)")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();
  
  
  @Override
  protected void init(final ProcessorInitializationContext context) {



    descriptors.add(WEAVER);


  }



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
