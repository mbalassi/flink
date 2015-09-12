package malom;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class Solver {

	public static void main(String[] args) throws Exception {

		System.out.println("VIGYAZAT! adjmasks atirva!");
		//System.out.println("VIGYAZAT! lose condition atirva!");

		Config.outPath = args[0];

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		PojoTypeInfo.registerCustomSerializer(GameState.class, GameState.GameStateSerializer.class);
		PojoTypeInfo.registerCustomSerializer(ValueCount.class, ValueCount.ValueCountSerializer.class);


//		ArrayList<SectorId> sectors = new ArrayList<SectorId>();
//		//sectors.add(new SectorId(3, 3, 0, 0));
//		//sectors.add(new SectorId(1, 1, 0, 0));
////		sectors.add(new SectorId(2, 3, 0, 0)); sectors.add(new SectorId(3, 2, 0, 0));
////		sectors.add(new SectorId(1, 3, 0, 0)); //sectors.add(new SectorId(3, 1, 0, 0));
//
//		sectors.add(new SectorId(3, 3, 0, 0));
//		sectors.add(new SectorId(2, 3, 0, 0));
//		//sectors.add(new SectorId(3, 4, 0, 0)); sectors.add(new SectorId(4, 3, 0, 0));
//
//		//-Xmx6g -Xms6g
//
//		Retrograde retr = new Retrograde(sectors, env);
//		Graph<GameState, ValueCount, NullValue> res = retr.run();
//		//res.getVertices().print();
//		//System.out.println(res.getVertices().count());
//
//		Verify.verify(res);
//
//		res.getVertices().writeAsText("/home/gabor/malom_output/res.txt", FileSystem.WriteMode.OVERWRITE);
//		env.execute();


		SectorGraph sectorGraph = new SectorGraph(env);
		sectorGraph.solve(new SectorId(3,4,0,0));

		env.execute();
		//System.out.println(env.getExecutionPlan());


//		List<Vertex<GameState, ValueCount>> resList = res.getVertices().collect();
//		Map<Long, ValueCount> resMap = new TreeMap<>();
//		for(Vertex<GameState, ValueCount> v: resList) {
//			resMap.put(v.getId().board, v.getValue());
//		}
//		Map<Long, Integer> v32 = new HashMap<>();
//		for(Vertex<GameState, ValueCount> v: resList) {
//			if(resMap.get(Symmetries.minSym48(v.getId().board)).value != v.getValue().value) {
//				int a = 42;
//			}
//
//			if(v.getValue().value == 32) {
//				Long x = Symmetries.minSym48(v.getId().board);
//				if(!v32.containsKey(x)) {
//					v32.put(x, 1);
//				} else {
//					v32.put(x, v32.get(x) + 1);
//				}
//			}
//		}
//		for(Object l: v32.entrySet()) {
//			System.out.println(l);
//		}
	}
}
