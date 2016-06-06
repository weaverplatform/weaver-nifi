package com.weaverplatform.nifi.individual;

import com.weaverplatform.nifi.WeaverProcessor;
import com.weaverplatform.nifi.util.WeaverProperties;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.NiFiProperties;

/**
 * @author Bastiaan Bijl
 */
public abstract class IndividualProcessor extends WeaverProcessor {

  String datasetId;


  public static final PropertyDescriptor DATASET = new PropertyDescriptor
      .Builder().name("dataset_id")
      .description("Dataset ID to add individuals to")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .build();

  public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    
    super.onTrigger(context, session);

    // Dataset
    if(context.getProperty(DATASET).getValue() != null) {
      datasetId = context.getProperty(DATASET).getValue();
    }
    else {
      datasetId = NiFiProperties.getInstance().get(WeaverProperties.DATASET).toString();
    }
  }
}
