package malom;

import java.nio.file.Paths;

public class Config {
	static final String movegenFile = "/tmp/movegen";

	static final int maxPieceCount = 9;
	static final String variantName = "std";

	static final boolean filterSym = true; // Filter by symmetries of the board (reduces state space to almost 1/16)


	static String outPath;
	static String verifyOutPath(SectorId sector) {
		return Paths.get(Config.outPath, "verif", sector.toString()).toString();
	}
	static String resultOutPath(SectorId sector) {
		return Paths.get(Config.outPath, "res", sector.toString()).toString();
	}
}
