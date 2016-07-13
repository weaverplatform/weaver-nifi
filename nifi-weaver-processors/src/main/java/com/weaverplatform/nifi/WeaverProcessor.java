package com.weaverplatform.nifi;

import com.weaverplatform.nifi.util.WeaverProperties;
import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.EntityNotFoundException;
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.model.Dataset;
import com.weaverplatform.sdk.websocket.WeaverSocket;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.NiFiProperties;

import java.net.URI;
import java.net.URISyntaxException;
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
  private Weaver weaver;

  public static final PropertyDescriptor WEAVER = new PropertyDescriptor
      .Builder().name("Weaver URL")
      .description("Weaver connection URL i.e. weaver.connect(url).")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();
  
  
  @Override
  protected void init(final ProcessorInitializationContext context) {
    
    getLogger().error("init started");

    descriptors.add(WEAVER);

    // Weaver URL
//    if(context.getProperty(WEAVER).isSet()) {
//      weaverUrl = context.getProperty(WEAVER).getValue();
//    } else {
      weaverUrl = NiFiProperties.getInstance().get(WeaverProperties.URL).toString();
//    }

    weaver = new Weaver();
    try {
      weaver.connect(new WeaverSocket(new URI(weaverUrl)));
    } catch (URISyntaxException e) {
      throw new ProcessException(e);
    }
  }
  
  public Weaver getWeaver() {
    return weaver;
  }

  public Entity getDataset() {
    String datasetId = NiFiProperties.getInstance().get(WeaverProperties.DATASET).toString();
    Entity dataset;
    try {
      dataset = weaver.get(datasetId);
    } catch(EntityNotFoundException e) {
      new Dataset(weaver, datasetId).create();
      dataset = weaver.get(datasetId);
    }
    return dataset;
  }

  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
  }

  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships.get();
  }

  @Override
  public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return properties;
  }
}
