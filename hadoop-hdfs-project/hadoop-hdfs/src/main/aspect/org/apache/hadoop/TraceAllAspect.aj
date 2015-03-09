package org.apache.hadoop;

import edu.brown.cs.systems.pivottracing.PivotTraced;
import edu.brown.cs.systems.pivottracing.PivotTracedMethod;

public aspect TraceAllAspect {
	declare @type : org.apache.hadoop..* : @PivotTraced;
	declare @method : public * org.apache.hadoop..*..*(..) : @PivotTracedMethod;
}