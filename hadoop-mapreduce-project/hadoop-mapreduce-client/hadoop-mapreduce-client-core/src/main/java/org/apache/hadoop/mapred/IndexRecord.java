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

import java.nio.ByteBuffer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

import edu.berkeley.xtrace.XTraceContext;
import edu.berkeley.xtrace.XTraceMetadata;
import edu.berkeley.xtrace.TaskID;

@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class IndexRecord {
  public long startOffset;
  public long rawLength;
  public long partLength;
  public XTraceMetadata m;

  public IndexRecord() { }

  public IndexRecord(long startOffset, long rawLength, long partLength, long xtrace_taskid, long xtrace_opid) {
    this.startOffset = startOffset;
    this.rawLength = rawLength;
    this.partLength = partLength;
    if (xtrace_taskid!=0 && xtrace_opid!=0) {
      byte[] taskid = ByteBuffer.allocate(8).putLong(xtrace_taskid).array();
      byte[] opid = ByteBuffer.allocate(8).putLong(xtrace_opid).array();
      m = new XTraceMetadata(new TaskID(taskid, 8), opid);
    }
  }
  
  public long getXTraceTaskID() {
    if (m!=null) {
      try {
        return ByteBuffer.wrap(m.getTaskId().get()).getLong();
      } catch (Exception e) {
      }
    }
    return 0L;
  }
  
  public long getXTraceOpID() {
    if (m!=null) {
      try {
        return ByteBuffer.wrap(m.getOpId()).getLong();
      } catch (Exception e) {
      }
    }
    return 0L;    
  }
  
  public void rememberContext() {
    m = XTraceContext.logMerge();
  }
  
  public void clearContext() {
    m = null;
  }
  
  public void joinContext() {
    XTraceContext.joinContext(m);
  }
  
  public boolean hasContext() {
    return m!=null;
  }
  
}
