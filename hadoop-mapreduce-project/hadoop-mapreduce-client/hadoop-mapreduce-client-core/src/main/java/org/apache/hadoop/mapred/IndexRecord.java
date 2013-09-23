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
package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import edu.brown.cs.systems.xtrace.Context;
import edu.brown.cs.systems.xtrace.Metadata.XTraceMetadata;
import edu.brown.cs.systems.xtrace.Metadata.XTraceMetadata.Builder;
import edu.brown.cs.systems.xtrace.XTrace;

@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class IndexRecord {
  public long startOffset;
  public long rawLength;
  public long partLength;
  public Context ctx;

  public IndexRecord() { }

  public IndexRecord(long startOffset, long rawLength, long partLength, long xtrace_taskid, long xtrace_opid) {
    this.startOffset = startOffset;
    this.rawLength = rawLength;
    this.partLength = partLength;
    if (xtrace_taskid!=0) {
      Builder builder = XTraceMetadata.newBuilder().setTaskID(xtrace_taskid);
      if (xtrace_opid != 0)
        builder.addParentEventID(xtrace_opid);
      ctx = Context.parse(builder.build().toByteArray());
    }
  }
  
  public long getXTraceTaskID() {
    if (ctx!=null) {
      try {
        return XTraceMetadata.parseFrom(ctx.bytes()).getTaskID();
      } catch (Exception e) {
      }
    }
    return 0L;
  }
  
  public long getXTraceOpID() {
    if (ctx!=null) {
      try {
        XTraceMetadata md = XTraceMetadata.parseFrom(ctx.bytes());
        if (md.getParentEventIDCount() > 0)
          return md.getParentEventID(0);
      } catch (Exception e) {
      }
    }
    return 0L;    
  }
  
  public void rememberContext() {
    ctx = XTrace.get();
  }
  
  public void clearContext() {
    ctx = null;
  }
  
  public void joinContext() {
    XTrace.join(ctx);
  }
  
  public boolean hasContext() {
    return ctx!=null;
  }
  
}
