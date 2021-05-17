package com.example;

import com.example.Example.NOI;
import com.google.cloud.functions.BackgroundFunction;
import com.google.cloud.functions.Context;
import java.util.Base64;
import java.util.Map;
import java.util.logging.Logger;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.LaunchTemplateParameters;
import com.google.api.services.dataflow.model.RuntimeEnvironment;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;

import java.io.BufferedWriter;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class Example implements BackgroundFunction<NOI> {
  private static final Logger logger = Logger.getLogger(Example.class.getName());
  private static final Gson gson = new Gson();
  private static final String noiType = "DATAFLOW_NOI";
  private static final String noiOperation = "TRIGGER";
  private static final String noiMetaName = "LEGACY_LOADPLAN";

  @Override
  public void accept(NOI message, Context context) {
    String data = message.data != null
      ? new String(Base64.getDecoder().decode(message.data))
      : "Hello, World";

    logger.info(data);
    String type= "";
    String operation = "";
    String metaName = "";
    try{
        JsonElement requestParsed = gson.fromJson(data, JsonElement.class);
        JsonObject requestJson = null;
        JsonObject noiJson = null;

        if (requestParsed != null && requestParsed.isJsonObject()) {
          requestJson = requestParsed.getAsJsonObject();
        
          if (requestJson != null && requestJson.has("noi")) {
            logger.info("noi");
            noiJson = requestJson.get("noi").getAsJsonObject();
            if ((noiJson != null) && (noiJson.has("type")) && (noiJson.has("operation")) && (noiJson.has("metaName"))){
              type = noiJson.get("type").getAsString();
              operation = noiJson.get("operation").getAsString();
              metaName = noiJson.get("metaName").getAsString();
              if((type.equals(noiType)) && (operation.equals(noiOperation)) && (metaName.equals(noiMetaName))){
                logger.info("equals parameter");
                HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
                JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
                GoogleCredential credential = GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);
                  if (credential.createScopedRequired()) {
                    credential = credential.createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
                }
                Dataflow dataflowService = new Dataflow.Builder(httpTransport, jsonFactory, credential)
                        .setApplicationName("Google Cloud Platform Sample")
                        .build();
                String projectId = "round-vent-306609";
                RuntimeEnvironment runtimeEnvironment = new RuntimeEnvironment();
                runtimeEnvironment.setBypassTempDirValidation(false);
                runtimeEnvironment.setTempLocation("gs://round-vent-306609/tmp/");

                LaunchTemplateParameters launchTemplateParameters = new LaunchTemplateParameters();
                launchTemplateParameters.setEnvironment(runtimeEnvironment);
                launchTemplateParameters.setJobName("newJob" + (new Date()).getTime());

              
                      
                Dataflow.Projects.Templates.Launch launch = dataflowService.projects().templates().launch(projectId, launchTemplateParameters);            
                // launch.setGcsPath("gs://round-vent-306609/template/app");
                // launch.execute();
              }
            }
          }
        }
     
    } catch (Exception e) {
      logger.severe("Error parsing JSON: " + e.getMessage());
    }
    logger.info("End");
  }

  public static class NOI {
    String data;
    Map<String, String> attributes;
    String messageId;
    String publishTime;
  }
}


