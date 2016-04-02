/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.mapreduce.v2.app.rm;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.mortbay.log.Log;


public class ContainerRequestEvent extends ContainerAllocatorEvent {
  
  private final Resource capability;
  private final String[] hosts;
  private final String[] racks;
  private boolean earlierAttemptFailed = false;
  //Declared Pattern for different attempts.
  String TaskAttempt = "attempt_[0-9]+_[0-9]+_m_[0-9]+_1";
  String TaskAttempt1 = "attempt_[0-9]+_[0-9]+_m_[0-9]+_2";
  String TaskAttempt2 = "attempt_[0-9]+_[0-9]+_m_[0-9]+_3";
  Pattern MapTask = Pattern.compile(TaskAttempt);
  Pattern MapTask1 = Pattern.compile(TaskAttempt1);
  Pattern MapTask2 = Pattern.compile(TaskAttempt2);
  
  
  public ContainerRequestEvent(TaskAttemptId attemptID, 
      Resource capability,
      String[] hosts, String[] racks) {
    super(attemptID, ContainerAllocator.EventType.CONTAINER_REQ);
    this.capability = capability;
    this.hosts = hosts;
    this.racks = racks;
  }
  
  ContainerRequestEvent(TaskAttemptId attemptID, Resource capability) {
    this(attemptID, capability, new String[0], new String[0]);
    this.earlierAttemptFailed = true;
    //Regex matching for task attempt number calculation
    Matcher m0 = MapTask.matcher(attemptID.toString());
    Matcher m1 = MapTask1.matcher(attemptID.toString());
    Matcher m2 = MapTask2.matcher(attemptID.toString());
    if(m0.find()){
    	Log.info("attemptID : " + attemptID + " " + " higher resource allocated " + capability);
    	this.capability.setMemory( 2*capability.getMemory());
    	Log.info("attemptID : " + attemptID + " " + " higher resource allocated" + capability);
    }
    if(m1.find()){
    	Log.info("attemptID : " + attemptID + " " + " higher resource allocated " + capability);
    	this.capability.setMemory( 3*capability.getMemory());
    	Log.info("attemptID : " + attemptID + " " + " higher resource allocated " + capability);
    }
    if(m2.find()){
    	Log.info("attemptID : " + attemptID + " " + " higher resource allocated " + capability);
    	this.capability.setMemory( 4*capability.getMemory());
    	Log.info("attemptID : " + attemptID + " " + " higher resource allocated " + capability);
    }
    this.earlierAttemptFailed = true;
  }

  
  public static ContainerRequestEvent createContainerRequestEventForFailedContainer(
      TaskAttemptId attemptID, 
      Resource capability) {
    //ContainerRequest for failed events does not consider rack / node locality?
	Log.info("Failed task, reallocating more memory : " + attemptID);
    return new ContainerRequestEvent(attemptID, capability);
  }

  public Resource getCapability() {
    return capability;
  }

  public String[] getHosts() {
    return hosts;
  }
  
  public String[] getRacks() {
    return racks;
  }
  
  public boolean getEarlierAttemptFailed() {
    return earlierAttemptFailed;
  }
}