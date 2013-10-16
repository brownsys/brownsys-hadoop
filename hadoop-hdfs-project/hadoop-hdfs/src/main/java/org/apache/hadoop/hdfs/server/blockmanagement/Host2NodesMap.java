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
package org.apache.hadoop.hdfs.server.blockmanagement;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.DFSUtil;

import edu.berkeley.xtrace.XTraceResourceTracing;

/** A map from host names to datanode descriptors. */
@InterfaceAudience.Private
@InterfaceStability.Evolving
class Host2NodesMap {
  private HashMap<String, DatanodeDescriptor[]> map
    = new HashMap<String, DatanodeDescriptor[]>();
  private ReadWriteLock hostmapLock = new ReentrantReadWriteLock();
  
  private void lockReadLock() {
    XTraceResourceTracing.requestLock(hostmapLock.readLock());
    hostmapLock.readLock().lock();
    XTraceResourceTracing.acquiredLock(hostmapLock.readLock());
  }
  
  private void unlockReadLock() {
    hostmapLock.readLock().unlock();
    XTraceResourceTracing.releasedLock(hostmapLock.readLock());
  }
  
  private void lockWriteLock() {
    XTraceResourceTracing.requestLock(hostmapLock.writeLock());
    hostmapLock.writeLock().lock();
    XTraceResourceTracing.acquiredLock(hostmapLock.writeLock());
  }
  
  private void unlockWriteLock() {
    hostmapLock.writeLock().unlock();
    XTraceResourceTracing.releasedLock(hostmapLock.writeLock());
  }

  /** Check if node is already in the map. */
  boolean contains(DatanodeDescriptor node) {
    if (node==null) {
      return false;
    }
      
    String ipAddr = node.getIpAddr();
    lockReadLock();
    try {
      DatanodeDescriptor[] nodes = map.get(ipAddr);
      if (nodes != null) {
        for(DatanodeDescriptor containedNode:nodes) {
          if (node==containedNode) {
            return true;
          }
        }
      }
    } finally {
      unlockReadLock();
    }
    return false;
  }
    
  /** add node to the map 
   * return true if the node is added; false otherwise.
   */
  boolean add(DatanodeDescriptor node) {
    lockWriteLock();
    try {
      if (node==null || contains(node)) {
        return false;
      }
      
      String ipAddr = node.getIpAddr();
      DatanodeDescriptor[] nodes = map.get(ipAddr);
      DatanodeDescriptor[] newNodes;
      if (nodes==null) {
        newNodes = new DatanodeDescriptor[1];
        newNodes[0]=node;
      } else { // rare case: more than one datanode on the host
        newNodes = new DatanodeDescriptor[nodes.length+1];
        System.arraycopy(nodes, 0, newNodes, 0, nodes.length);
        newNodes[nodes.length] = node;
      }
      map.put(ipAddr, newNodes);
      return true;
    } finally {
      unlockWriteLock();
    }
  }
    
  /** remove node from the map 
   * return true if the node is removed; false otherwise.
   */
  boolean remove(DatanodeDescriptor node) {
    if (node==null) {
      return false;
    }
      
    String ipAddr = node.getIpAddr();
    lockWriteLock();
    try {

      DatanodeDescriptor[] nodes = map.get(ipAddr);
      if (nodes==null) {
        return false;
      }
      if (nodes.length==1) {
        if (nodes[0]==node) {
          map.remove(ipAddr);
          return true;
        } else {
          return false;
        }
      }
      //rare case
      int i=0;
      for(; i<nodes.length; i++) {
        if (nodes[i]==node) {
          break;
        }
      }
      if (i==nodes.length) {
        return false;
      } else {
        DatanodeDescriptor[] newNodes;
        newNodes = new DatanodeDescriptor[nodes.length-1];
        System.arraycopy(nodes, 0, newNodes, 0, i);
        System.arraycopy(nodes, i+1, newNodes, i, nodes.length-i-1);
        map.put(ipAddr, newNodes);
        return true;
      }
    } finally {
      unlockWriteLock();
    }
  }
    
  /**
   * Get a data node by its IP address.
   * @return DatanodeDescriptor if found, null otherwise 
   */
  DatanodeDescriptor getDatanodeByHost(String ipAddr) {
    if (ipAddr == null) {
      return null;
    }
      
    lockReadLock();
    try {
      DatanodeDescriptor[] nodes = map.get(ipAddr);
      // no entry
      if (nodes== null) {
        return null;
      }
      // one node
      if (nodes.length == 1) {
        return nodes[0];
      }
      // more than one node
      return nodes[DFSUtil.getRandom().nextInt(nodes.length)];
    } finally {
      unlockReadLock();
    }
  }
  
  /**
   * Find data node by its transfer address
   *
   * @return DatanodeDescriptor if found or null otherwise
   */
  public DatanodeDescriptor getDatanodeByXferAddr(String ipAddr,
      int xferPort) {
    if (ipAddr==null) {
      return null;
    }

    lockReadLock();
    try {
      DatanodeDescriptor[] nodes = map.get(ipAddr);
      // no entry
      if (nodes== null) {
        return null;
      }
      for(DatanodeDescriptor containedNode:nodes) {
        if (xferPort == containedNode.getXferPort()) {
          return containedNode;
        }
      }
      return null;
    } finally {
      unlockReadLock();
    }
  }

  @Override
  public String toString() {
    final StringBuilder b = new StringBuilder(getClass().getSimpleName())
        .append("[");
    for(Map.Entry<String, DatanodeDescriptor[]> e : map.entrySet()) {
      b.append("\n  " + e.getKey() + " => " + Arrays.asList(e.getValue()));
    }
    return b.append("\n]").toString();
  }
}
