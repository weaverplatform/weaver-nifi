package com.weaverplatform.nifi.individual;

import com.weaverplatform.nifi.util.WeaverProperties;
import com.weaverplatform.sdk.Entity;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.NiFiProperties;

/**
 * @author Bastiaan Bijl
 */
public abstract class DatasetProcessor extends FlowFileProcessor {

  String datasetId;
  Entity dataset;
  Entity datasetObjects;

  public static final PropertyDescriptor DATASET = new PropertyDescriptor
      .Builder().name("Dataset ID")
      .description("Dataset ID to add individuals to.")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();


  @Override
  protected void init(final ProcessorInitializationContext context) {

    super.init(context);

    descriptors.add(DATASET);

  }

  @Override
  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

    super.onTrigger(context, session);

    if(context.getProperty(DATASET).getValue() != null) {
      datasetId = context.getProperty(DATASET).getValue();
    } else {
      datasetId = NiFiProperties.getInstance().get(WeaverProperties.DATASET).toString();
    }
    //log.info("will use this dataset "+datasetId);
    final ProcessorLog log = this.getLogger();
    log.error(datasetId);

    try {
        dataset = weaver.get(datasetId);
        datasetObjects = weaver.get(dataset.getRelations().get("objects").getId());
    } catch(NullPointerException e) {
        log.error(e.getMessage());
    }
  }


}