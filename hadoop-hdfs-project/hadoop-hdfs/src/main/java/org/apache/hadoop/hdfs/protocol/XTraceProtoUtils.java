package org.apache.hadoop.hdfs.protocol;

import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientReadStatusProto;

import com.google.protobuf.ByteString;

import edu.brown.cs.systems.xtrace.XTrace;

/**
 * Contains some utility functions for XTrace instrumentation.  Saves having to repeat
 * instrumentation in loads of places in the code.
 * @author jon
 *
 */
public class XTraceProtoUtils {
  
  /**
   * Shortcut method to create a new builder, then insert the current XTraceContext into it
   * @return
   */
  public static BlockOpResponseProto.Builder newBlockOpResponseProtoBuilder() {
    BlockOpResponseProto.Builder b = BlockOpResponseProto.newBuilder();
    setXtrace(b);
    return b;
  }
  
  /**
   * If the current XTraceContext is valid, sets it in the provided builder
   * @param builder
   */
  public static void setXtrace(BlockOpResponseProto.Builder builder) {
    if (XTrace.active())
      builder.setXtrace(ByteString.copyFrom(XTrace.bytes()));
  }
  
  /**
   * Joins an XTrace context if this message contains one
   * @param p
   */
  public static void join(BlockOpResponseProto p) {
    if (p.hasXtrace())
      XTrace.join(p.getXtrace().toByteArray());
  }
  
  /**
   * Shortcut method to create a new builder, then insert the current XTraceContext into it
   * @return
   */
  public static ClientReadStatusProto.Builder newClientReadStatusProtoBuilder() {
    ClientReadStatusProto.Builder b = ClientReadStatusProto.newBuilder();
    setXtrace(b);
    return b;
  }
  
  /**
   * If the current XTraceContext is valid, sets it in the provided builder
   * @param builder
   */
  public static void setXtrace(ClientReadStatusProto.Builder builder) {
    if (XTrace.active())
      builder.setXtrace(ByteString.copyFrom(XTrace.bytes()));
  }
  
  /**
   * Joins an XTrace context if this message contains one
   * @param p
   */
  public static void join(ClientReadStatusProto p) {
    if (p.hasXtrace())
      XTrace.join(p.getXtrace().toByteArray());
  }
  
  
}
