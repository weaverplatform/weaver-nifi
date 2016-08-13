package com.weaverplatform.nifi.individual;

import com.google.gson.Gson;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

/**
 * Created by Moose on 04/08/16.
 */
public class FlowErrorCatcher {

  final ProcessContext context;
  final ProcessSession session;
  final String path;

  String processorId;

  public FlowErrorCatcher(ProcessContext cntxt, ProcessSession sess, String id) {
    context = cntxt;
    session = sess;
    processorId = id;
    path = "FlowErrors";
    
    File dir = new File(path);
    if (!dir.exists()){
      dir.mkdir();
    }
  }

  public void dump(FlowFile flowFile) {

    if (true) return;
    
    //Write flowfile to error heap, and send it through the flow without any other processing
    String fileName = processorId + "  ***  " +  context.getName() + "  ***  " +  flowFile.getLastQueueDate();
    File file = new File(new File(path), fileName);
    try {
      Files.write(file.toPath(), new Gson().toJson(flowFile).getBytes(), StandardOpenOption.CREATE);
    } catch (IOException e) {
      System.out.println("Couldn't write file " + e);
    }
  }
}
