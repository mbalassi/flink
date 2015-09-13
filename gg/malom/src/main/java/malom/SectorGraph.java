package malom;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This class represents the subdivision of the state space into sectors, and drives the computation,
 * by calling into the Retrograde class.
 * The concept of "sector graph" means a graph where the vertices are the sectors, and there is a directed edge from
 * u sector to v sector, if there is a game state in u from where you can make a movoe to a game state in v.
 *
 * The computation will go one work unit at a time. A work unit is either one non-twin sector (see sectorId.hasTwin), or
 * two sectors that are twins of each other. (Assuming a more general sector graph, the work units should be the
 * strongly connected components. This is the case here: the only loops are self-edges and back-and-forth edges between
 * twin nodes.).
 */
public class SectorGraph {

	ExecutionEnvironment env;

	private Map<SectorId, DataSet<Vertex<GameState, ValueCount>>> results = new HashMap<>();

	public SectorGraph(ExecutionEnvironment env) {
		this.env = env;
	}

	/**
	 * Compute the game-theoretical solution for all states of the work unit of the given sector.
	 * Recursively calls itself for sectors that the work unit directly depends on, and uses memoization (the results map).
	 * (Note: we don't actually compute anything here when the method is called; we are just creating Flink DataSets.)
	 */
	public DataSet<Vertex<GameState, ValueCount>> solve(SectorId u) {
		if(u.isLosing()) {
			results.put(u, Retrograde.createSectorVertices(u, ValueCount.value(Value.loss(0)), env));
		} else {

			// The main sectors are the one or two sectors of the work unit.
			// The child sectors are those sectors, that the main sectors directly depend on.
			// The sector family is all these together.

			Set<SectorId> mainSectors = new HashSet<>(Arrays.asList(u, u.negate()));
			Set<SectorId> chdSectors = new HashSet<>();
			for(SectorId mainSec: mainSectors) {
				chdSectors.addAll(graphFunc(mainSec).
						stream().filter(chdSec -> !mainSectors.contains(chdSec)).
						collect(Collectors.toList()));
			}
			assert chdSectors.size() > 0;
			List<SectorId> sectorFamily = new ArrayList<>();
			sectorFamily.addAll(mainSectors);
			sectorFamily.addAll(chdSectors);

			// The vertices DataSet will consist of all the vertices of the sector family.
			// - vertices of child sectors will have been already solved
			// - vertices of main sectors will be initialized to undefined

			DataSet<Vertex<GameState, ValueCount>> vertices = null;

			for(SectorId chdSector: chdSectors) {
				DataSet<Vertex<GameState, ValueCount>> currentSectorVertices = solve(chdSector);
				if(vertices == null) {
					vertices = currentSectorVertices;
				} else {
					vertices = vertices.union(currentSectorVertices);
				}
			}

			// Add the main sectors
			vertices = vertices.union(Retrograde.createSectorVertices(u, ValueCount.count(-1), env));
			if(u.hasTwin()) {
				vertices = vertices.union(Retrograde.createSectorVertices(u.negate(), ValueCount.count(-1), env));
			}

			// Create the edges
			DataSet<Edge<GameState, NullValue>> edges = Retrograde.createEdges(vertices, sectorFamily, mainSectors);

			Graph<GameState, ValueCount, NullValue> g = Graph.fromDataSet(vertices, edges, env);

			// Initialize main sectors (we do this here, because we need the degrees)
			g = Retrograde.init(g, u, u.hasTwin() ? u.negate() : null, env);

			// The essence of the computation
			g = Retrograde.iterate(g);

			// We need a result DataSet for each of the main sectors.
			results.put(u, g.getVertices().filter(new FilterToOneSector(u)));
			if(u.hasTwin()) {
				results.put(u.negate(), g.getVertices().filter(new FilterToOneSector(u.negate())));
			}

			// Write the result files
			results.get(u).writeAsText(Config.resultOutPath(u), FileSystem.WriteMode.OVERWRITE);
			if(u.hasTwin()) {
				results.get(u.negate()).writeAsText(Config.resultOutPath(u.negate()), FileSystem.WriteMode.OVERWRITE);
			}

			// Compute verify and write it
			Verify.verify(g, u, u.hasTwin() ? u.negate() : null);
		}
		return results.get(u);
	}


	public static class FilterToOneSector implements FilterFunction<Vertex<GameState, ValueCount>> {

		SectorId s;

		public FilterToOneSector(SectorId s) {
			this.s = s;
		}

		@Override
		public boolean filter(Vertex<GameState, ValueCount> value) throws Exception {
			return value.getId().sid.equals(s);
		}
	}


	// Returns the outgoing edges from a sector
	private List<SectorId> graphFunc(SectorId u) {
		List<SectorId> r0 = graphFunc0(u);
		List<SectorId> r = new ArrayList<>();
		for (SectorId x : r0) {
			// TODO: filtering (see std_filtered in Sporol.java)
			if(x.b >= 0) {
				x.negateInPlace(); // Game states are negated after each move. (see comment in SectorId.java)
				r.add(x);
			}
		}
		return r;
	}

	private List<SectorId> graphFunc0(SectorId u) {
		// In the standard variant, there are two outgoing edges from each node: we either close a mill, or not.
		List<SectorId> v = new ArrayList<>();
		v.add(new SectorId(u));
		v.add(new SectorId(u));

		if (u.wf != 0) { // Can we place a stone?
			// Not taking a stone.
			v.get(0).wf--;
			v.get(0).w++;

			// Taking a stone
			v.get(1).wf--;
			v.get(1).w++;
			v.get(1).b--;
		} else { // Moving a stone
			v.get(1).b--; // Taking a stone
			// (sectorId doesn't chang when we don't take a stone) (negation will be handled by graphFunc)
		}

		return v;
	}

}
