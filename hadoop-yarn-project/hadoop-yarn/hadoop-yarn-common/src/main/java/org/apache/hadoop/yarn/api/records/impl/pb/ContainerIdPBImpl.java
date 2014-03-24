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

package org.apache.hadoop.yarn.api.records.impl.pb;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.proto.YarnProtos.ApplicationAttemptIdProto;
import org.apache.hadoop.yarn.proto.YarnProtos.ContainerIdProto;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;

import edu.brown.cs.systems.xtrace.Context;
import edu.brown.cs.systems.xtrace.XTrace;

@Private
@Unstable
public class ContainerIdPBImpl extends ContainerId {
  ContainerIdProto proto = null;
  ContainerIdProto.Builder builder = null;
  private ApplicationAttemptId applicationAttemptId = null;
  Context xmd = null;

  public ContainerIdPBImpl() {
    builder = ContainerIdProto.newBuilder();
  }

  public ContainerIdPBImpl(ContainerIdProto proto) {
    this.proto = proto;
    this.applicationAttemptId = convertFromProtoFormat(proto.getAppAttemptId());
    if (proto!=null && proto.hasXtrace()) {
      xmd = Context.parse(proto.getXtrace().toByteArray());
    }    
  }
  
  public ContainerIdProto getProto() {
    return proto;
  }

  @Override
  public int getId() {
    Preconditions.checkNotNull(proto);
    return proto.getId();
  }

  @Override
  protected void setId(int id) {
    Preconditions.checkNotNull(builder);
    builder.setId((id));
  }


  @Override
  public ApplicationAttemptId getApplicationAttemptId() {
    return this.applicationAttemptId;
  }

  @Override
  protected void setApplicationAttemptId(ApplicationAttemptId atId) {
    if (atId != null) {
      Preconditions.checkNotNull(builder);
      builder.setAppAttemptId(convertToProtoFormat(atId));
    }
    this.applicationAttemptId = atId;
  }

  @Override
  public void rememberContext() {
    xmd = XTrace.get();
  }
  
  @Override
  public void joinContext() {
    XTrace.join(xmd);
  }

  private ApplicationAttemptIdPBImpl convertFromProtoFormat(
      ApplicationAttemptIdProto p) {
    return new ApplicationAttemptIdPBImpl(p);
  }

  private ApplicationAttemptIdProto convertToProtoFormat(
      ApplicationAttemptId t) {
    return ((ApplicationAttemptIdPBImpl)t).getProto();
  }

  @Override
  protected void build() {
    if (xmd!=null) {
      builder.setXtrace(ByteString.copyFrom(xmd.bytes()));
    }
    proto = builder.build();
    builder = null;
  }
}  
