package malom;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;

public class Solver {

	public static void main(String[] args) throws Exception {

		System.out.println("VIGYAZAT! adjmasks atirva!");
		//System.out.println("VIGYAZAT! lose condition atirva!");


		Config.outPath = args[0];

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		PojoTypeInfo.registerCustomSerializer(GameState.class, GameState.GameStateSerializer.class);
		PojoTypeInfo.registerCustomSerializer(ValueCount.class, ValueCount.ValueCountSerializer.class);

		//-Xmx8g -Xms8g

		SectorGraph sectorGraph = new SectorGraph(env);
		sectorGraph.solve(new SectorId(3,4,0,0));
		//sectorGraph.solve(new SectorId(5,5,0,0));

		env.execute();
		//System.out.println(env.getExecutionPlan());
	}
}
