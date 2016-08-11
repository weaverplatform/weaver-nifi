package com.weaverplatform.nifi.individual;

import com.google.gson.Gson;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.io.InputStreamCallback;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    path = "/Users/Moose/FlowErrors/";
  }

  public void dump(FlowFile flowFile) {

    //Write flowfile to error heap, and send it through the flow without any other processing
    String fileName = processorId + "  ***  " +  context.getName() + "  ***  " +  flowFile.getLastQueueDate();
    Path file = Paths.get(path + fileName);
    try {
      Files.write(file, new Gson().toJson(flowFile).getBytes(), StandardOpenOption.CREATE);
    } catch (IOException e) {
      System.out.println("Couldn't write file " + e);
    }
  }
}
