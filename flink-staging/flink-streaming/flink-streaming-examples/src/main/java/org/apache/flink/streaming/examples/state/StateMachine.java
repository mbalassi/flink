/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.state;

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Describes the states and the transition used for fraud detection.
 */
public class StateMachine implements Serializable {

	public enum STATE {IDLE, FORWARD, BACKWARD}
	public enum TRANSITION {SOUND, TOUCH}

	private Map<STATE, Map<TRANSITION, STATE>> stateTransitions = new HashMap<STATE, Map<TRANSITION, STATE>>() {
		private static final long serialVersionUID = 2239464794963721781L;
		{
		put(STATE.IDLE, ImmutableMap.of(TRANSITION.SOUND, STATE.FORWARD));
		put(STATE.FORWARD, ImmutableMap.of(TRANSITION.SOUND, STATE.BACKWARD, TRANSITION.TOUCH, STATE.IDLE));
		put(STATE.BACKWARD, ImmutableMap.of(TRANSITION.SOUND, STATE.FORWARD, TRANSITION.TOUCH, STATE.IDLE));
		}
	};

	private STATE currentState = STATE.IDLE;

	public STATE transition(TRANSITION transition) {
		if (stateTransitions.containsKey(currentState)) {
			Map<TRANSITION, STATE> availableEdges = stateTransitions.get(currentState);
			if (availableEdges.containsKey(transition)) {
				currentState = availableEdges.get(transition);
				return currentState;
			} else {
				throw new StateMachineException("Cannot transition from " + currentState + " with " + transition + ".");
			}
		} else {
			throw new StateMachineException("Cannot transition from " + currentState + " with " + transition + ".");
		}
	}

	public TRANSITION generate(){
		if (stateTransitions.containsKey(currentState)){
			Map<TRANSITION, STATE> availableEdges = stateTransitions.get(currentState);
			if (availableEdges.size() > 0){
				// Put available transitions to an indexable structure to choose randomly
				ArrayList<TRANSITION> transitions = new ArrayList<TRANSITION>(availableEdges.keySet());
				Random random = new Random();

				TRANSITION transition = transitions.get(random.nextInt(transitions.size()));
				currentState = availableEdges.get(transition);
				return transition;
			} else {
				throw new StateMachineException("Cannot generate from " + currentState + ". It is a sink.");
			}
		} else {
			throw new StateMachineException("Cannot generate from " + currentState + ". It is a sink.");
		}
	}

	public STATE getState(){
		return this.currentState;
	}

	public void setState(STATE state){
		this.currentState = state;
	}

	public class StateMachineException extends RuntimeException {
		public StateMachineException(String message) {
			super(message);
		}
	}

}
