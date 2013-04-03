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

package org.apache.hadoop.yarn.state;

import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.yarn.state.StateMachineFactory.Trace;
import org.apache.hadoop.yarn.util.Graph;

import edu.berkeley.xtrace.XTraceContext;
import edu.berkeley.xtrace.XTraceMetadata;
import edu.berkeley.xtrace.XTraceEvent;

/**
 * State machine topology.
 * This object is semantically immutable.  If you have a
 * StateMachineFactory there's no operation in the API that changes
 * its semantic properties.
 *
 * @param <OPERAND> The object type on which this state machine operates.
 * @param <STATE> The state of the entity.
 * @param <EVENTTYPE> The external eventType to be handled.
 * @param <EVENT> The event object.
 *
 */
final public class StateMachineFactory
             <OPERAND, STATE extends Enum<STATE>,
              EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
  
  public enum Trace { NEVER, ALWAYS, KEEPALIVE, IGNORE }

  private final TransitionsListNode transitionsListNode;

  private Map<STATE, Map<EVENTTYPE,
    Transition<OPERAND, STATE, EVENTTYPE, EVENT>>> stateMachineTable;

  private STATE defaultInitialState;

  private final boolean optimized;
  
  /**
   * X-Trace variables
   */
  private int xtrace_id_seed = 1;
  private Trace xtrace_default = Trace.ALWAYS;

  /**
   * Constructor (Default MR)
   *
   */
  public StateMachineFactory(STATE defaultInitialState) {
    this(defaultInitialState, Trace.ALWAYS);
  }

  /**
   * Constructor (X-Trace)
   *
   */
  public StateMachineFactory(STATE defaultInitialState, Trace xtrace_default) {
    this.transitionsListNode = null;
    this.defaultInitialState = defaultInitialState;
    this.optimized = false;
    this.stateMachineTable = null;
    this.xtrace_default = xtrace_default;
  }
  
  private StateMachineFactory
      (StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> that,
       ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> t) {
    this.defaultInitialState = that.defaultInitialState;
    this.transitionsListNode 
        = new TransitionsListNode(t, that.transitionsListNode);
    this.optimized = false;
    this.stateMachineTable = null;
    this.xtrace_default = that.xtrace_default;
  }

  private StateMachineFactory
      (StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> that,
       boolean optimized) {
    this.defaultInitialState = that.defaultInitialState;
    this.transitionsListNode = that.transitionsListNode;
    this.optimized = optimized;
    if (optimized) {
      makeStateMachineTable();
    } else {
      stateMachineTable = null;
    }
    this.xtrace_default = that.xtrace_default;
  }

  private interface ApplicableTransition
             <OPERAND, STATE extends Enum<STATE>,
              EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
    void apply(StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject);
  }

  private class TransitionsListNode {
    final ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition;
    final TransitionsListNode next;

    TransitionsListNode
        (ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> transition,
        TransitionsListNode next) {
      this.transition = transition;
      this.next = next;
    }
  }

  static private class ApplicableSingleOrMultipleTransition
             <OPERAND, STATE extends Enum<STATE>,
              EVENTTYPE extends Enum<EVENTTYPE>, EVENT>
          implements ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT> {
    final STATE preState;
    final EVENTTYPE eventType;
    final Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition;

    ApplicableSingleOrMultipleTransition
        (STATE preState, EVENTTYPE eventType,
         Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition) {
      this.preState = preState;
      this.eventType = eventType;
      this.transition = transition;
    }

    @Override
    public void apply
             (StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> subject) {
      Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap
        = subject.stateMachineTable.get(preState);
      if (transitionMap == null) {
        // I use HashMap here because I would expect most EVENTTYPE's to not
        //  apply out of a particular state, so FSM sizes would be 
        //  quadratic if I use EnumMap's here as I do at the top level.
        transitionMap = new HashMap<EVENTTYPE,
          Transition<OPERAND, STATE, EVENTTYPE, EVENT>>();
        subject.stateMachineTable.put(preState, transitionMap);
      }
      transitionMap.put(eventType, transition);
    }
  }

  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition.  This overload
   *          has no hook object.
   *
   *         Note that the returned StateMachineFactory is a distinct
   *         object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postState post-transition state
   * @param eventType stimulus for the transition
   */
  public StateMachineFactory
             <OPERAND, STATE, EVENTTYPE, EVENT>
          addTransition(STATE preState, STATE postState, EVENTTYPE eventType) {
    return addTransition(preState, postState, eventType, xtrace_default);
  }

  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition.  This overload
   *          has no hook object.
   *
   *         Note that the returned StateMachineFactory is a distinct
   *         object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postState post-transition state
   * @param eventType stimulus for the transition
   * @param xtrace_process boolean flag, indicating whether the transition should be logged as an xtrace process with an edge to the previous transition (or not)
   */
  public StateMachineFactory
             <OPERAND, STATE, EVENTTYPE, EVENT>
          addTransition(STATE preState, STATE postState, EVENTTYPE eventType, Trace xtrace_process) {
    return addTransition(preState, postState, eventType, null, xtrace_process);
  }

  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition.  This overload
   *          has no hook object.
   *
   *
   *         Note that the returned StateMachineFactory is a distinct
   *         object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postState post-transition state
   * @param eventTypes List of stimuli for the transitions
   */
  public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(
      STATE preState, STATE postState, Set<EVENTTYPE> eventTypes) {
    return addTransition(preState, postState, eventTypes, xtrace_default);
  }

  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition.  This overload
   *          has no hook object.
   *
   *
   *         Note that the returned StateMachineFactory is a distinct
   *         object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postState post-transition state
   * @param eventTypes List of stimuli for the transitions
   * @param xtrace_process boolean flag, indicating whether the transition should be logged as an xtrace process with an edge to the previous transition (or not)
   */
  public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(
      STATE preState, STATE postState, Set<EVENTTYPE> eventTypes, Trace xtrace_process) {
    return addTransition(preState, postState, eventTypes, null, xtrace_process);
  }

  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition
   *
   *         Note that the returned StateMachineFactory is a distinct
   *         object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postState post-transition state
   * @param eventTypes List of stimuli for the transitions
   * @param hook transition hook
   */
  public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(
      STATE preState, STATE postState, Set<EVENTTYPE> eventTypes,
      SingleArcTransition<OPERAND, EVENT> hook) {
    return addTransition(preState, postState, eventTypes, hook, xtrace_default);
  }

  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition
   *
   *         Note that the returned StateMachineFactory is a distinct
   *         object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postState post-transition state
   * @param eventTypes List of stimuli for the transitions
   * @param hook transition hook
   * @param xtrace_process boolean flag, indicating whether the transition should be logged as an xtrace process with an edge to the previous transition (or not)
   */
  public StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> addTransition(
      STATE preState, STATE postState, Set<EVENTTYPE> eventTypes,
      SingleArcTransition<OPERAND, EVENT> hook, Trace xtrace_process) {
    StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT> factory = null;
    for (EVENTTYPE event : eventTypes) {
      if (factory == null) {
        factory = addTransition(preState, postState, event, hook, xtrace_process);
      } else {
        factory = factory.addTransition(preState, postState, event, hook, xtrace_process);
      }
    }
    return factory;
  }

  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition
   *
   *         Note that the returned StateMachineFactory is a distinct object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postState post-transition state
   * @param eventType stimulus for the transition
   * @param hook transition hook
   */
  public StateMachineFactory
             <OPERAND, STATE, EVENTTYPE, EVENT>
          addTransition(STATE preState, STATE postState,
                        EVENTTYPE eventType,
                        SingleArcTransition<OPERAND, EVENT> hook){
    return addTransition(preState, postState, eventType, hook, xtrace_default);
  }

  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition
   *
   *         Note that the returned StateMachineFactory is a distinct object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postState post-transition state
   * @param eventType stimulus for the transition
   * @param hook transition hook
   * @param xtrace_process boolean flag, indicating whether the transition should be logged as an xtrace process with an edge to the previous transition (or not)
   */
  public StateMachineFactory
             <OPERAND, STATE, EVENTTYPE, EVENT>
          addTransition(STATE preState, STATE postState,
                        EVENTTYPE eventType,
                        SingleArcTransition<OPERAND, EVENT> hook, Trace xtrace_process){
    return new StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>
        (this, new ApplicableSingleOrMultipleTransition<OPERAND, STATE, EVENTTYPE, EVENT>
           (preState, eventType, new SingleInternalArc(postState, hook, xtrace_process)));
  }

  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition
   *
   *         Note that the returned StateMachineFactory is a distinct object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postStates valid post-transition states
   * @param eventType stimulus for the transition
   * @param hook transition hook
   */
  public StateMachineFactory
             <OPERAND, STATE, EVENTTYPE, EVENT>
          addTransition(STATE preState, Set<STATE> postStates,
                        EVENTTYPE eventType,
                        MultipleArcTransition<OPERAND, EVENT, STATE> hook){
    return addTransition(preState, postStates, eventType, hook, xtrace_default);
  }

  /**
   * @return a NEW StateMachineFactory just like {@code this} with the current
   *          transition added as a new legal transition
   *
   *         Note that the returned StateMachineFactory is a distinct object.
   *
   *         This method is part of the API.
   *
   * @param preState pre-transition state
   * @param postStates valid post-transition states
   * @param eventType stimulus for the transition
   * @param hook transition hook
   * @param xtrace_process boolean flag, indicating whether the transition should be logged as an xtrace process with an edge to the previous transition (or not)
   */
  public StateMachineFactory
             <OPERAND, STATE, EVENTTYPE, EVENT>
          addTransition(STATE preState, Set<STATE> postStates,
                        EVENTTYPE eventType,
                        MultipleArcTransition<OPERAND, EVENT, STATE> hook, Trace xtrace_process){
    return new StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>
        (this,
         new ApplicableSingleOrMultipleTransition<OPERAND, STATE, EVENTTYPE, EVENT>
           (preState, eventType, new MultipleInternalArc(postStates, hook, xtrace_process)));
  }

  /**
   * @return a StateMachineFactory just like {@code this}, except that if
   *         you won't need any synchronization to build a state machine
   *
   *         Note that the returned StateMachineFactory is a distinct object.
   *
   *         This method is part of the API.
   *
   *         The only way you could distinguish the returned
   *         StateMachineFactory from {@code this} would be by
   *         measuring the performance of the derived 
   *         {@code StateMachine} you can get from it.
   *
   * Calling this is optional.  It doesn't change the semantics of the factory,
   *   if you call it then when you use the factory there is no synchronization.
   */
  public StateMachineFactory
             <OPERAND, STATE, EVENTTYPE, EVENT>
          installTopology() {
    return new StateMachineFactory<OPERAND, STATE, EVENTTYPE, EVENT>(this, true);
  }

  /**
   * Effect a transition due to the effecting stimulus.
   * @param state current state
   * @param eventType trigger to initiate the transition
   * @param cause causal eventType context
   * @return transitioned state
   */
  private STATE doTransition
           (OPERAND operand, STATE oldState, EVENTTYPE eventType, EVENT event)
      throws InvalidStateTransitonException {
    // We can assume that stateMachineTable is non-null because we call
    //  maybeMakeStateMachineTable() when we build an InnerStateMachine ,
    //  and this code only gets called from inside a working InnerStateMachine .
    Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap
      = stateMachineTable.get(oldState);
    if (transitionMap != null) {
      Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition
          = transitionMap.get(eventType);
      if (transition != null) {
        return transition.doTransition(operand, oldState, event, eventType);
      }
    }
    throw new InvalidStateTransitonException(oldState, eventType);
  }
  
  private boolean logXTraceTransition(STATE oldState, EVENTTYPE eventType) {
    Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap
    = stateMachineTable.get(oldState);
    if (transitionMap != null) {
      Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition
          = transitionMap.get(eventType);
      if (transition != null) {
        Trace trace_type = transition.getTraceType();
        return trace_type!=Trace.IGNORE;
      }
    }
    return true;    
  }
  
  /**
   * Tells us whether the transition should link back to the previous transition event
   * @param oldState current state
   * @param eventType trigger to initiate the transition
   * @return true if the transition should link the previous xtrace context with this one
   */
  private boolean logTransitionAsXTraceProcess(STATE oldState, EVENTTYPE eventType) {
    Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitionMap
    = stateMachineTable.get(oldState);
    if (transitionMap != null) {
      Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition
          = transitionMap.get(eventType);
      if (transition != null) {
        Trace trace_type = transition.getTraceType();
        return trace_type==Trace.ALWAYS || (trace_type==Trace.KEEPALIVE && !XTraceContext.isValid());
      }
    }
    return false;
  }

  private synchronized void maybeMakeStateMachineTable() {
    if (stateMachineTable == null) {
      makeStateMachineTable();
    }
  }

  private void makeStateMachineTable() {
    Stack<ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>> stack =
      new Stack<ApplicableTransition<OPERAND, STATE, EVENTTYPE, EVENT>>();

    Map<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>
      prototype = new HashMap<STATE, Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>();

    prototype.put(defaultInitialState, null);

    // I use EnumMap here because it'll be faster and denser.  I would
    //  expect most of the states to have at least one transition.
    stateMachineTable
       = new EnumMap<STATE, Map<EVENTTYPE,
                           Transition<OPERAND, STATE, EVENTTYPE, EVENT>>>(prototype);

    for (TransitionsListNode cursor = transitionsListNode;
         cursor != null;
         cursor = cursor.next) {
      stack.push(cursor.transition);
    }

    while (!stack.isEmpty()) {
      stack.pop().apply(this);
    }
  }

  private interface Transition<OPERAND, STATE extends Enum<STATE>,
          EVENTTYPE extends Enum<EVENTTYPE>, EVENT> {
    STATE doTransition(OPERAND operand, STATE oldState,
                       EVENT event, EVENTTYPE eventType);

    Trace getTraceType();
  }

  private class SingleInternalArc
                    implements Transition<OPERAND, STATE, EVENTTYPE, EVENT> {

    private STATE postState;
    private SingleArcTransition<OPERAND, EVENT> hook; // transition hook
    private Trace xtrace_process;

    SingleInternalArc(STATE postState,
        SingleArcTransition<OPERAND, EVENT> hook,
        Trace xtrace_process) {
      this.postState = postState;
      this.hook = hook;
      this.xtrace_process = xtrace_process;
    }

    @Override
    public STATE doTransition(OPERAND operand, STATE oldState,
                              EVENT event, EVENTTYPE eventType) {
      if (hook != null) {
        hook.transition(operand, event);
      }
      return postState;
    }

    @Override
    public Trace getTraceType() {
      return xtrace_process;
    }
  }

  private class MultipleInternalArc
              implements Transition<OPERAND, STATE, EVENTTYPE, EVENT>{

    // Fields
    private Set<STATE> validPostStates;
    private MultipleArcTransition<OPERAND, EVENT, STATE> hook;  // transition hook
    private Trace xtrace_process;

    MultipleInternalArc(Set<STATE> postStates,
                   MultipleArcTransition<OPERAND, EVENT, STATE> hook,
                   Trace xtrace_process) {
      this.validPostStates = postStates;
      this.hook = hook;
      this.xtrace_process = xtrace_process;
    }

    @Override
    public STATE doTransition(OPERAND operand, STATE oldState,
                              EVENT event, EVENTTYPE eventType)
        throws InvalidStateTransitonException {
      STATE postState = hook.transition(operand, event);

      if (!validPostStates.contains(postState)) {
        throw new InvalidStateTransitonException(oldState, eventType);
      }
      return postState;
    }

    @Override
    public Trace getTraceType() {
      return xtrace_process;
    }
  }

  /* 
   * @return a {@link StateMachine} that starts in 
   *         {@code initialState} and whose {@link Transition} s are
   *         applied to {@code operand} .
   *
   *         This is part of the API.
   *
   * @param operand the object upon which the returned 
   *                {@link StateMachine} will operate.
   * @param initialState the state in which the returned 
   *                {@link StateMachine} will start.
   *                
   */
  public StateMachine<STATE, EVENTTYPE, EVENT>
        make(OPERAND operand, STATE initialState) {
    return new InternalStateMachine(operand, initialState, xtrace_id_seed++);
  }

  /* 
   * @return a {@link StateMachine} that starts in the default initial
   *          state and whose {@link Transition} s are applied to
   *          {@code operand} . 
   *
   *         This is part of the API.
   *
   * @param operand the object upon which the returned 
   *                {@link StateMachine} will operate.
   *                
   */
  public StateMachine<STATE, EVENTTYPE, EVENT> make(OPERAND operand) {
    return new InternalStateMachine(operand, defaultInitialState, xtrace_id_seed++);
  }
  
  private String xtraceStateName(STATE state) {
    return state.getDeclaringClass().getSimpleName() + "." + state.name();
  }

  private class InternalStateMachine
        implements StateMachine<STATE, EVENTTYPE, EVENT> {
    private final OPERAND operand;
    private STATE currentState;

    private String state_machine_id;
    private Collection<XTraceMetadata> previous_transition_context;

    InternalStateMachine(OPERAND operand, STATE initialState, int xtrace_id_seed) {
      this.operand = operand;
      this.currentState = initialState;
      if (!optimized) {
        maybeMakeStateMachineTable();
      }
      this.state_machine_id = operand.getClass().getSimpleName()+"-"+xtrace_id_seed;
      XTraceContext.logEvent(operand.getClass(), operand.getClass().getSimpleName()+" init", "StateMachine initialized", 
          "StartState", xtraceStateName(currentState), "StateMachineID", state_machine_id);
      this.previous_transition_context = XTraceContext.getThreadContext();
    }

    @Override
    public synchronized STATE getCurrentState() {
      return currentState;
    }    
    
    public synchronized STATE doTransition(EVENTTYPE eventType, EVENT event)
        throws InvalidStateTransitonException {
      // Add an edge to the previous state machine transition if the transition is to be treated as an xtrace process
      boolean set_previous_transition_context = false;
      if (StateMachineFactory.this.logXTraceTransition(currentState, eventType)) {
        if (StateMachineFactory.this.logTransitionAsXTraceProcess(currentState, eventType)) {
          XTraceContext.joinContext(this.previous_transition_context);
        }
        
        // Create an event for the transition
        XTraceContext.logEvent(operand.getClass(), operand.getClass().getSimpleName()+event.toString(), event.toString(), 
            "StartState", xtraceStateName(currentState), "Operand", operand.toString(), "StateMachineID", state_machine_id);
        
        set_previous_transition_context = true;
      }
      
      // Do the transition
      try {
        currentState = StateMachineFactory.this.doTransition(operand, currentState, eventType, event);
      } catch (InvalidStateTransitonException e) {
        throw e;
      }

      if (set_previous_transition_context && XTraceContext.isValid()) {
        this.previous_transition_context = XTraceContext.getThreadContext();
      }
      
      return currentState;
    }
  }

  /**
   * Generate a graph represents the state graph of this StateMachine
   * @param name graph name
   * @return Graph object generated
   */
  public Graph generateStateGraph(String name) {
    maybeMakeStateMachineTable();
    Graph g = new Graph(name);
    for (STATE startState : stateMachineTable.keySet()) {
      Map<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> transitions
          = stateMachineTable.get(startState);
      for (Entry<EVENTTYPE, Transition<OPERAND, STATE, EVENTTYPE, EVENT>> entry :
         transitions.entrySet()) {
        Transition<OPERAND, STATE, EVENTTYPE, EVENT> transition = entry.getValue();
        if (transition instanceof StateMachineFactory.SingleInternalArc) {
          StateMachineFactory.SingleInternalArc sa
              = (StateMachineFactory.SingleInternalArc) transition;
          Graph.Node fromNode = g.getNode(startState.toString());
          Graph.Node toNode = g.getNode(sa.postState.toString());
          fromNode.addEdge(toNode, entry.getKey().toString());
        } else if (transition instanceof StateMachineFactory.MultipleInternalArc) {
          StateMachineFactory.MultipleInternalArc ma
              = (StateMachineFactory.MultipleInternalArc) transition;
          Iterator iter = ma.validPostStates.iterator();
          while (iter.hasNext()) {
            Graph.Node fromNode = g.getNode(startState.toString());
            Graph.Node toNode = g.getNode(iter.next().toString());
            fromNode.addEdge(toNode, entry.getKey().toString());
          }
        }
      }
    }
    return g;
  }
}
