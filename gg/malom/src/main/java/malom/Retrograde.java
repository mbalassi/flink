package malom;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.MessagingFunction;
import org.apache.flink.graph.spargel.VertexCentricConfiguration;
import org.apache.flink.graph.spargel.VertexUpdateFunction;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Retrograde implements Serializable {
	private static final long serialVersionUID = 1L;

	public static DataSet<Vertex<GameState, ValueCount>> createSectorVertices(SectorId s, ValueCount vv, ExecutionEnvironment env) {
		DataSet<GameState> gameStates = env.fromCollection(new SectorElementIterator(s), GameState.class);
		if(Config.filterSym) {
			gameStates = gameStates.filter(state -> !Symmetries.isFiltered(state.board));
		}
		return gameStates.map(new MapFunction<GameState, Vertex<GameState, ValueCount>>() {
			@Override
			public Vertex<GameState, ValueCount> map(GameState s) throws Exception {
				return new Vertex<>(s, vv);
			}
		}).name("Vertices " + s);
	}

	public static DataSet<Edge<GameState, NullValue>> createEdges(DataSet<Vertex<GameState, ValueCount>> vertices, List<SectorId> sectors) {
		Movegen movegen = new Movegen(sectors);
		return vertices.flatMap(new FlatMapFunction<Vertex<GameState, ValueCount>, Edge<GameState, NullValue>>() {
			@Override
			public void flatMap(Vertex<GameState, ValueCount> v, Collector<Edge<GameState, NullValue>> out) throws Exception {
				if(!Config.filterSym) {
					for (GameState parent : movegen.get_parents(v.getId())) {
						out.collect(new Edge<>(v.getId(), parent, new NullValue()));
					}
				} else {
					assert !Symmetries.isFiltered(v.getId().board);
					Set<Long> parentBoards = new HashSet<>();
					for (GameState parent : movegen.get_parents(v.getId())) {
						parent.board = Symmetries.minSym48(parent.board);
						if(!parentBoards.contains(parent.board)) {
							out.collect(new Edge<>(v.getId(), parent, new NullValue()));
							parentBoards.add(parent.board);
						}
					}
				}
			}
		}).name("Edges");
	}

	/**
	 * Initialize main sectors:
	 *  - states where I can't make a move are losses in 0
	 *  - other states are initialized to count(inDegree)
	 *
	 *  @param g		A graph of a sector family, where vertices of the main sectors are not initialized
	 *  @param mainSec1	One of the main sectors
	 *  @param mainSec2 The other main sector (null, if not twin)
	 *  @return		g transformed, by initializing the vertices of the main sectors.
 	 */
	public static Graph<GameState, ValueCount, NullValue> countChdAndInitBlocked(
			Graph<GameState, ValueCount, NullValue> g,
			SectorId mainSec1, SectorId mainSec2,
			ExecutionEnvironment env) {
		// Set losses to 0, and
		// set others to count(deg)
		return Graph.fromDataSet(g.getVertices().join(g.inDegrees(), JoinOperatorBase.JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo(0).with(new JoinFunction<Vertex<GameState, ValueCount>, Tuple2<GameState, Long>, Vertex<GameState, ValueCount>>() {
			@Override
			public Vertex<GameState, ValueCount> join(Vertex<GameState, ValueCount> vertex, Tuple2<GameState, Long> deg0) throws Exception {
				long deg = deg0.f1;
				GameState v = vertex.f0;
				if (!v.sid.equals(mainSec1) && (mainSec2 == null || !v.sid.equals(mainSec2))) {
					return vertex;
				} else {
					if (deg == 0) { // state is blocked
						return new Vertex<>(v, ValueCount.value(Value.loss(0)));
					} else { // to be computed by the iteration (set to count for now)
						// (Note: the first iteration will take child sectors into account.)
						return new Vertex<>(v, ValueCount.count((int) deg));
					}
				}
			}
		}), g.getEdges(), env);
	}

	/**
	 * The core of the computation. We iterate by the order of increasing depth (cf. the assert with getSuperstepNumber).
	 * Each vertex sends msg at most once, when its final value (loss or win) is determined. (Vertices in child sectors
	 * do this in the first iteration.)
	 * Draw states never send a msg.
	 *
	 * @param g		A graph of a sector family, where the child sectors have already been solved, and the main sectors
	 *              have been initialized by countChdAndInitBlocked.
	 * @return		g transformed, so that the vertices of the main sectors are solved (have their final values).
	 */
	public static Graph<GameState, ValueCount, NullValue> iterate(Graph<GameState, ValueCount, NullValue> g) {

		VertexCentricConfiguration config = new VertexCentricConfiguration();
		//config.setOptDegrees(true);
		//config.setSolutionSetUnmanagedMemory(true);

		return g.runVertexCentricIteration(new VertexUpdateFunction<GameState, ValueCount, Value>() {
			@Override
			public void updateVertex(Vertex<GameState, ValueCount> vertex, MessageIterator<Value> inMessages) throws Exception {
				boolean newValueSet = false;
				ValueCount vv = vertex.getValue();
				for (Value msg : inMessages) {
					assert getSuperstepNumber() == msg.depth + 1;
					if (vv.isCount()) { //count-ba terjesztunk
						if (msg.isWin()) { //nyeresbol terjesztunk
							short newCount = (short) (vv.count - 1);
							if (newCount == 0) { //elfogyott a count
								// We have seen all successors, set final value. (will be a loss, because msg is a win)
								vv = ValueCount.value(msg.undoNegate());
								newValueSet = true;
							} else { //nem fogyott el a count
								// One less successor to wait for
								vv = ValueCount.count(newCount);
								newValueSet = true;
							}
						} else { //vesztesbol terjesztunk
							// We can move to a loss, so we have a win.
							vv = ValueCount.value(msg.undoNegate());
							newValueSet = true;
						}
					} else { //val-ba terjesztunk
						// This can't be a loss. We set a state to loss only after seeing all its successors, so
						// why haven't we seen the successor that this msg is coming from?
						assert vv.value.isWin();
						// We shouldn't have found a better value for a state that we already think we know the final value of.
						assert vv.value.depth <= msg.depth + 1;
					}
				}
				if(newValueSet) {
					setNewVertexValue(vv);
				}
			}
		}, new MessagingFunction<GameState, ValueCount, Value, NullValue>() {
			@Override
			public void sendMessages(Vertex<GameState, ValueCount> vertex) throws Exception {
				// Note: if we were coding the iteration manually (without Gelly),
				// we could stuff the below "if" into a filter to be done before joining the workset with the edges.
				if (vertex.getValue().isValue()) {
					sendMessageToAllNeighbors(vertex.getValue().value);
					assert vertex.getValue().value.depth + 1 == getSuperstepNumber();
				}
			}
		}, 1000, config);
	}
}
