package com.weaverplatform.nifi;

import com.weaverplatform.nifi.util.WeaverProperties;
import com.weaverplatform.sdk.Entity;
import com.weaverplatform.sdk.EntityNotFoundException;
import com.weaverplatform.sdk.Weaver;
import com.weaverplatform.sdk.json.request.ReadPayload;
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

  private static String weaverUrl = null;
  private static Weaver weaver = null;
  private static Entity dataset = null;
  private static Entity datasetObjects = null;
  private static Entity datasetViews = null;

  public static final PropertyDescriptor WEAVER = new PropertyDescriptor
      .Builder().name("Weaver URL")
      .description("Weaver connection URL i.e. weaver.connect(url).")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();
  
  
  @Override
  protected void init(final ProcessorInitializationContext context) {
    descriptors.add(WEAVER);
  }
  
  public Weaver getWeaver() {
    if(weaverUrl == null) {
      weaverUrl = NiFiProperties.getInstance().get(WeaverProperties.URL).toString();
    }
    if(weaver == null) {
      weaver = new Weaver();
      try {
        weaver.connect(new WeaverSocket(new URI(weaverUrl)));
      } catch (URISyntaxException e) {
        throw new ProcessException(e);
      }
    }
    return weaver;
  }

  public Entity getDatasetObjects() {

    if(datasetObjects != null) {
      return datasetObjects;
    }

    Weaver weaver = getWeaver();

    if(dataset == null) {
      String datasetId = NiFiProperties.getInstance().get(WeaverProperties.DATASET).toString();
      try {
        dataset = weaver.get(datasetId, new ReadPayload.Opts(1));
      } catch(EntityNotFoundException e) {
        new Dataset(weaver, datasetId).get(datasetId);
        dataset = weaver.get(datasetId);
      }
    }

    if(datasetObjects == null) {
      datasetObjects = weaver.get(dataset.getRelations().get("objects").getId(), new ReadPayload.Opts(0));
    }

    return datasetObjects;
  }

  public Entity getDatasetViews() {

    if(datasetObjects != null) {
      return datasetObjects;
    }

    Weaver weaver = getWeaver();

    if(dataset == null) {
      String datasetId = NiFiProperties.getInstance().get(WeaverProperties.DATASET).toString();
      try {
        dataset = weaver.get(datasetId);
      } catch(EntityNotFoundException e) {
        new Dataset(weaver, datasetId).get(datasetId);
        dataset = weaver.get(datasetId);
      }
    }

    if(datasetObjects == null) {
      datasetObjects = weaver.get(dataset.getRelations().get("views").getId(), new ReadPayload.Opts(0));
    }

    return datasetObjects;
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
