/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package dataproc;
// [START dataproc_instantiate_inline_workflow_template]
import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.dataproc.v1.ClusterConfig;
import com.google.cloud.dataproc.v1.GceClusterConfig;
import com.google.cloud.dataproc.v1.PigJob;
import com.google.cloud.dataproc.v1.ManagedCluster;
import com.google.cloud.dataproc.v1.OrderedJob;
import com.google.cloud.dataproc.v1.RegionName;
import com.google.cloud.dataproc.v1.WorkflowMetadata;
import com.google.cloud.dataproc.v1.WorkflowTemplate;
import com.google.cloud.dataproc.v1.WorkflowTemplatePlacement;
import com.google.cloud.dataproc.v1.WorkflowTemplateServiceClient;
import com.google.cloud.dataproc.v1.WorkflowTemplateServiceSettings;
import com.google.protobuf.Empty;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import com.google.api.client.googleapis.auth.oauth2.*;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
// Importing HashMap class
import java.util.HashMap;
import java.sql.Timestamp;

public class PigWorkflowTemplate {

  public static void instantiateInlineWorkflowTemplate(HashMap options) throws IOException, InterruptedException {
    // TODO(developer): Replace these variables before running the sample.
    String projectId = "aryabhatta-dev";
    String region = "us-central1";
    System.out.println("options");
    System.out.println(options);
    String pigScript = (String) options.get("pigScript");
    String inputDir = (String) options.get("inputDir");
    String outputDir = (String) options.get("outputDir");

    // The below code is to fetch date and hour from input cloud storage path
    String[] pathElements = inputDir.split("/");
    String dateDir = pathElements[pathElements.length-2];
    String date = dateDir.substring(dateDir.lastIndexOf('=') + 1);
    String hourDir = pathElements[pathElements.length-1];
    String hour = hourDir.substring(hourDir.lastIndexOf('=') + 1);
    String inputTime = date + " " + hour + ":00:00";
    System.out.println("Input Time: " + inputTime);
    Timestamp timestamp = Timestamp.valueOf(inputTime);
    System.out.println(timestamp);
    instantiateInlineWorkflowTemplate(projectId, region, pigScript, inputDir, outputDir, timestamp.toString());
  }

  public static void instantiateInlineWorkflowTemplate(String projectId, String region, String pigScript, String inputDir, String outputDir, String inputTime)
      throws IOException, InterruptedException {
    String myEndpoint = String.format("%s-dataproc.googleapis.com:443", region);

    // Configure the settings for the workflow template service client.
    WorkflowTemplateServiceSettings workflowTemplateServiceSettings =
        WorkflowTemplateServiceSettings.newBuilder().setEndpoint(myEndpoint).build();

    // Create a workflow template service client with the configured settings. The client only
    // needs to be created once and can be reused for multiple requests. Using a try-with-resources
    // closes the client, but this can also be done manually with the .close() method.
    try (WorkflowTemplateServiceClient workflowTemplateServiceClient =
        WorkflowTemplateServiceClient.create(workflowTemplateServiceSettings)) {

      // Configure the jobs within the workflow.
      PigJob uimPigJob =
              PigJob.newBuilder()
                      .setQueryFileUri(pigScript)
                      .addJarFileUris("gs://phw-code/phw.jar")
                      .addJarFileUris("gs://phw-code/piggybank-0.12.0-cdh5.16.2.jar")
                      .addJarFileUris("gs://phw-code/cdh5-pig12-orcstorage-1.0.3.jar")
                      .putScriptVariables("inputdir", inputDir)
                      .putScriptVariables("outputdir", outputDir)
                      .putScriptVariables("inputtime", inputTime)
                      .build();
      OrderedJob uimpig =
              OrderedJob.newBuilder().setPigJob(uimPigJob).setStepId("hour00").build();
      System.out.println("Added Pigjob jars and options.");
      // Configure the cluster placement for the workflow.
      // Leave "ZoneUri" empty for "Auto Zone Placement".
      // GceClusterConfig gceClusterConfig =
      //     GceClusterConfig.newBuilder().setZoneUri("").build();
      GceClusterConfig gceClusterConfig =
          GceClusterConfig.newBuilder().setZoneUri("us-central1-a").build();
      ClusterConfig clusterConfig =
          ClusterConfig.newBuilder().setGceClusterConfig(gceClusterConfig).build();
      ManagedCluster managedCluster =
          ManagedCluster.newBuilder()
              .setClusterName("my-managed-cluster")
              .setConfig(clusterConfig)
              .build();
      WorkflowTemplatePlacement workflowTemplatePlacement =
          WorkflowTemplatePlacement.newBuilder().setManagedCluster(managedCluster).build();
      System.out.println("Set my-managed-cluster to workflow template.");
      // Create the inline workflow template.
      WorkflowTemplate workflowTemplate =
          WorkflowTemplate.newBuilder()
              .addJobs(uimpig)
              .setPlacement(workflowTemplatePlacement)
              .build();
      System.out.println("Build workflow template.");
      // Submit the instantiated inline workflow template request.
      String parent = RegionName.format(projectId, region);
      OperationFuture<Empty, WorkflowMetadata> instantiateInlineWorkflowTemplateAsync =
          workflowTemplateServiceClient.instantiateInlineWorkflowTemplateAsync(
              parent, workflowTemplate);
      instantiateInlineWorkflowTemplateAsync.get();

      // Print out a success message.
      System.out.printf("Workflow ran successfully.");

    } catch (ExecutionException e) {
      System.err.println(String.format("Error running workflow: %s ", e.getMessage()));
    }
  }

  public  static void main(String args[]){
    try{
      HashMap<String, String> options = new HashMap<>();
      options.put("inputDir", System.getProperty("inputDir"));
      options.put("outputDir", System.getProperty("outputDir"));
      options.put("pigScript", System.getProperty("pigScript"));

      System.out.println("inputDir : "+ options.get("inputDir"));
      System.out.println("outputDir : "+ options.get("outputDir"));
      System.out.println("pigScript : "+ options.get("pigScript"));

      PigWorkflowTemplate.instantiateInlineWorkflowTemplate(options);
    } catch (Exception e){
      e.printStackTrace(System.out);
      System.err.println(String.format("Error running workflow main: %s ", e.getMessage()));
    }

  }
}
// [END dataproc_instantiate_inline_workflow_template]
