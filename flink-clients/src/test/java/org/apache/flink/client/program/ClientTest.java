/*
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

package org.apache.flink.client.program;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
<<<<<<< HEAD
import akka.actor.Props;
import akka.actor.Status;
import akka.actor.UntypedActor;
=======
import org.apache.flink.api.java.ExecutionEnvironment;
>>>>>>> 3846301d4e945da56acb6e0f5828401c6047c6c2
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.Plan;
<<<<<<< HEAD
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.CostEstimator;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
=======
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.costs.CostEstimator;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plantranslate.NepheleJobGraphGenerator;
>>>>>>> 3846301d4e945da56acb6e0f5828401c6047c6c2
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.client.JobClient$;
import org.apache.flink.runtime.jobgraph.JobGraph;
<<<<<<< HEAD
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.net.NetUtils;
import org.junit.After;

=======
import org.apache.flink.runtime.messages.JobManagerMessages;
>>>>>>> 3846301d4e945da56acb6e0f5828401c6047c6c2
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;
import scala.Tuple2;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.powermock.api.mockito.PowerMockito.doAnswer;
import static org.powermock.api.mockito.PowerMockito.whenNew;


/**
 * Simple and maybe stupid test to check the {@link Client} class.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({Client.class, JobClient$.class})
public class ClientTest {

<<<<<<< HEAD
	private PackagedProgram program;
	private Optimizer compilerMock;
	private JobGraphGenerator generatorMock;


	private Configuration config;

	private ActorSystem jobManagerSystem;

	private JobGraph jobGraph = new JobGraph("test graph");
=======
	@Mock Configuration configMock;
	@Mock PackagedProgram program;
	@Mock JobWithJars planWithJarsMock;
	@Mock Plan planMock;
	@Mock PactCompiler compilerMock;
	@Mock OptimizedPlan optimizedPlanMock;
	@Mock NepheleJobGraphGenerator generatorMock;
	@Mock JobGraph jobGraphMock;
	@Mock ActorSystem mockSystem;
	@Mock JobClient$ mockJobClient;
	@Mock JobManagerMessages.SubmissionSuccess mockSubmissionSuccess;
	@Mock JobManagerMessages.SubmissionFailure mockSubmissionFailure;
	@Mock ActorRef mockJobClientActor;
>>>>>>> 3846301d4e945da56acb6e0f5828401c6047c6c2

	@Before
	public void setUp() throws Exception {
		initMocks(this);

<<<<<<< HEAD
		final int freePort = NetUtils.getAvailablePort();
		config = new Configuration();
		config.setString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, "localhost");
		config.setInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, freePort);
		config.setString(ConfigConstants.AKKA_ASK_TIMEOUT, ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT);

		program = mock(PackagedProgram.class);
		compilerMock = mock(Optimizer.class);
		generatorMock = mock(JobGraphGenerator.class);

		JobWithJars planWithJarsMock = mock(JobWithJars.class);
		Plan planMock = mock(Plan.class);
		OptimizedPlan optimizedPlanMock = mock(OptimizedPlan.class);
=======
		when(configMock.getString(ConfigConstants.JOB_MANAGER_IPC_ADDRESS_KEY, null)).thenReturn("localhost");
		when(configMock.getInteger(ConfigConstants.JOB_MANAGER_IPC_PORT_KEY, ConfigConstants.DEFAULT_JOB_MANAGER_IPC_PORT)).thenReturn(6123);
		when(configMock.getString(ConfigConstants.AKKA_ASK_TIMEOUT, ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT)).thenReturn(ConfigConstants.DEFAULT_AKKA_ASK_TIMEOUT);
>>>>>>> 3846301d4e945da56acb6e0f5828401c6047c6c2

		when(planMock.getJobName()).thenReturn("MockPlan");
//		when(mockJarFile.getAbsolutePath()).thenReturn("mockFilePath");

		when(program.getPlanWithJars()).thenReturn(planWithJarsMock);
		when(planWithJarsMock.getPlan()).thenReturn(planMock);

		whenNew(Optimizer.class).withArguments(any(DataStatistics.class), any(CostEstimator.class)).thenReturn(this.compilerMock);
		when(compilerMock.compile(planMock)).thenReturn(optimizedPlanMock);

<<<<<<< HEAD
		whenNew(JobGraphGenerator.class).withNoArguments().thenReturn(generatorMock);
		when(generatorMock.compileJobGraph(optimizedPlanMock)).thenReturn(jobGraph);

		try {
			Tuple2<String, Object> address = new Tuple2<String, Object>("localhost", freePort);
			jobManagerSystem = AkkaUtils.createActorSystem(config, new Some<Tuple2<String, Object>>(address));
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Setup of test actor system failed.");
		}
	}
=======
		whenNew(NepheleJobGraphGenerator.class).withNoArguments().thenReturn(generatorMock);
		when(generatorMock.compileJobGraph(optimizedPlanMock)).thenReturn(jobGraphMock);
>>>>>>> 3846301d4e945da56acb6e0f5828401c6047c6c2

		Whitebox.setInternalState(JobClient$.class, mockJobClient);

		when(mockJobClient.startActorSystemAndActor(configMock, false)).thenReturn(new Tuple2<ActorSystem,
				ActorRef>(mockSystem, mockJobClientActor));
	}

	@Test
	public void shouldSubmitToJobClient() throws ProgramInvocationException, IOException {
		when(mockJobClient.submitJobDetached(any(JobGraph.class),
				any(ActorRef.class), any(FiniteDuration.class))).thenReturn(mockSubmissionSuccess);

<<<<<<< HEAD
			try {
				out.run(program.getPlanWithJars(), -1, false);
				fail("This should fail with an exception");
			}
			catch (ProgramInvocationException e) {
				// bam!
			}
			catch (Exception e) {
				fail("wrong exception " + e);
			}
=======
		Client out = new Client(configMock, getClass().getClassLoader());
		out.run(program.getPlanWithJars(), -1, false);
		program.deleteExtractedLibraries();
>>>>>>> 3846301d4e945da56acb6e0f5828401c6047c6c2

		verify(this.compilerMock, times(1)).compile(planMock);
		verify(this.generatorMock, times(1)).compileJobGraph(optimizedPlanMock);
	}

	@Test(expected = ProgramInvocationException.class)
	public void shouldThrowException() throws Exception {
		when(mockJobClient.submitJobDetached(any(JobGraph.class),
				any(ActorRef.class), any(FiniteDuration.class))).thenReturn(mockSubmissionFailure);

		Client out = new Client(configMock, getClass().getClassLoader());
		out.run(program.getPlanWithJars(), -1, false);
		program.deleteExtractedLibraries();
	}

	@Test(expected = InvalidProgramException.class)
	public void tryLocalExecution() throws Exception {
		PackagedProgram packagedProgramMock = mock(PackagedProgram.class);

<<<<<<< HEAD
		@Override
		public void onReceive(Object message) throws Exception {
			getSender().tell(new Status.Success(new JobID()), getSelf());
		}
	}
=======
		when(packagedProgramMock.isUsingInteractiveMode()).thenReturn(true);
>>>>>>> 3846301d4e945da56acb6e0f5828401c6047c6c2

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				ExecutionEnvironment.createLocalEnvironment();
				return null;
			}
		}).when(packagedProgramMock).invokeInteractiveModeForExecution();

<<<<<<< HEAD
		@Override
		public void onReceive(Object message) throws Exception {
			getSender().tell(new Status.Failure(new Exception("test")), getSelf());
		}
=======
		new Client(configMock, getClass().getClassLoader()).run(packagedProgramMock, 1, true);
>>>>>>> 3846301d4e945da56acb6e0f5828401c6047c6c2
	}
}
