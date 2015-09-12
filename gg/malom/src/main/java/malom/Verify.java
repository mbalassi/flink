package malom;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

// Verifies the result of a Retrograde run for a sector family
public class Verify {
	static public void verify(Graph<GameState, ValueCount, NullValue> g, SectorId mainSec1, SectorId mainSec2) {
		DataSet<Tuple4<GameState, ValueCount, ValueCount, GameState>> verif = g.groupReduceOnNeighbors(
				new NeighborsFunctionWithVertexValue<GameState, ValueCount, NullValue, Tuple4<GameState, ValueCount, ValueCount, GameState>>() {
			@Override
			public void iterateNeighbors(
					Vertex<GameState, ValueCount> vertex,
					Iterable<Tuple2<Edge<GameState, NullValue>, Vertex<GameState, ValueCount>>> neighbors,
					Collector<Tuple4<GameState, ValueCount, ValueCount, GameState>> out) throws Exception {

				if(!vertex.getId().sid.equals(mainSec1) && (mainSec2 == null || !vertex.getId().sid.equals(mainSec2))) {
					return;
				}

				ValueCount vv = vertex.getValue();

				if(vertex.getId().sid.isLosing()) {
					if(!vv.isValue() || !vv.value.equals(Value.loss(0))) {
						out.collect(Tuple4.of(vertex.getId(), ValueCount.value(Value.loss(0)), vv, GameState.getNull()));
					}
				}

				boolean blocked = true, hasDraw = false, hasWin = false, hasLoss = false;
				short bestWin = Short.MAX_VALUE, bestLoss = Short.MIN_VALUE;
				GameState drawMh = null, winMh = null, lossMh = null;
				int numChd = 0, numWinChd = 0;
				for (Tuple2<Edge<GameState, NullValue>, Vertex<GameState, ValueCount>> n : neighbors) {
					blocked = false;
					numChd++;
					ValueCount cvc = n.f1.getValue();
					GameState cid = n.f1.getId();
					if(cvc.isCount()) { // child is draw
						hasDraw = true;
						drawMh = cid;
					} else if(cvc.isValue() && cvc.value.isLoss()) { // child is loss
						hasWin = true;
						if(cvc.value.depth + 1 < bestWin) {
							bestWin = (short)(cvc.value.depth + 1);
							winMh = cid;
						}
					} else if(cvc.isValue() && cvc.value.isWin()) { // child is win
						hasLoss = true;
						if(cvc.value.depth + 1 > bestLoss) {
							bestLoss = (short)(cvc.value.depth + 1);
							lossMh = cid;
						}
						numWinChd++;
					} else {
						assert false;
					}
				}

				ValueCount shouldBe = null;
				GameState mh = null;
				if(blocked) {
					shouldBe = ValueCount.value(Value.loss(0));
				} else {
					if(hasWin) {
						shouldBe = ValueCount.value(Value.win(bestWin));
						mh = winMh;
					} else if(hasDraw) {
						shouldBe = ValueCount.count(numChd - numWinChd);
						mh = drawMh;
					} else if(hasLoss) {
						shouldBe = ValueCount.value(Value.loss(bestLoss));
						mh = lossMh;
					} else {
						assert false;
					}
				}

				if(!vv.equals(shouldBe)) {
					out.collect(Tuple4.of(vertex.getId(), shouldBe, vv, mh));
				}
			}
		}, EdgeDirection.IN);

		verif.filter(new FilterFunction<Tuple4<GameState, ValueCount, ValueCount, GameState>>() {
			@Override
			public boolean filter(Tuple4<GameState, ValueCount, ValueCount, GameState> v) throws Exception {
				return v.f0.sid.equals(mainSec1);
			}
		}).writeAsText(Config.verifyOutPath(mainSec1), FileSystem.WriteMode.OVERWRITE);
		if(mainSec2 != null) {
			verif.filter(new FilterFunction<Tuple4<GameState, ValueCount, ValueCount, GameState>>() {
				@Override
				public boolean filter(Tuple4<GameState, ValueCount, ValueCount, GameState> v) throws Exception {
					return v.f0.sid.equals(mainSec2);
				}
			}).writeAsText(Config.verifyOutPath(mainSec2), FileSystem.WriteMode.OVERWRITE);
		}

	}
}
