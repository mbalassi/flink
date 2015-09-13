/*
Malom, a Nine Men's Morris (and variants) player and solver program.
Copyright(C) 2007-2014  Gabor E. Gevay, Gabor Danner

See our webpage (and the paper linked from there):
http://compalg.inf.elte.hu/~ggevay/mills/index.php


This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package malom;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class Movegen implements Serializable {
	private static final long serialVersionUID = 1L;

	static final int[] millpos = new int[]{14,56,224,131,3584,14336,57344,33536,917504,3670016,14680064,8585216,65793,263172,1052688,4210752};
	static final int[] slide_adjmasks = new int[]{386,5,1034,20,4136,80,16544,65,98817,1280,264708,5120,1058832,20480,4235328,16640,8519936,327680,656384,1310720,2625536,5242880,10502144,4259840}; //mezok szomszedainak maskjai
	static final int[] fly_adjmasks = new int[]{0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF,0xFFFFFF};

	static final int[] millmasks = new int[1<<24]; //lookup table a malomban levo korongok maskjaira
	static final int[] untakemasks = new int[1<<24]; //lookup table arra, hogy hova lehet visszatenni korongot
	static final int[] millclosemasks = new int[1<<24]; //lookup table arra, hogy mely poziciokra rakva zarodna be malom

	static final long mask24 = (1<<24) - 1;

	static {
		System.out.println("Movegen static init");
		try {
			if(new File(Config.movegenFile).exists()) {
				read_movegen();
			} else {
				write_movegen();
			}
		} catch (IOException e) {
			throw new RuntimeException("IOException when initializing movegen");
		}
	}

	static void write_movegen() throws IOException {
		System.out.println("write_movegen calculating lookuptables");

		System.out.println("  millmasks");
//		for(int a = 0; a < 1<<24; a++)
//			for(int i = 0; i < millpos.length; i++)
//				if((a & millpos[i]) == millpos[i])
//					millmasks[a] |= millpos[i];
		for(int a = 0; a < 1<<24; a++)
			for (int mp : millpos)
				if ((a & mp) == mp)
					millmasks[a] |= mp;

		System.out.println("  untakemasks");
		for(int a = 0; a < 1<<24; a++){
			int r = (~a) & (int)mask24; //alapbol ures helyekre lehet visszatenni
			for (int mp : millpos) {
				int amp = a & mp;

				if (Integer.bitCount(amp) == 2) { //ket korongos malompozicio
					//azt kell meg ellenorizni, hogy csak akkor ussuk ki a mezot, ha nem lenne minden malomban, ha oda visszatennenk a korongot
					int mm = amp ^ mp; //a mezo maskja
					if (!(millmasks[a | mm] == (a | mm)))
						r &= ~mp;
				}
			}

			untakemasks[a] = r;
		}

		System.out.println("  millclosemasks");
		for(int a=0; a<1<<24; a++){
			for (int m : millpos) {
				int am = a & m;
				if (Integer.bitCount(am) == 2)
					millclosemasks[a] |= m ^ am;
			}
		}

		System.out.print("Writing to file...");
		ObjectOutputStream f = null;
		try {
			f = new ObjectOutputStream(new FileOutputStream(Config.movegenFile));
			f.writeObject(millmasks);
			f.writeObject(untakemasks);
			f.writeObject(millclosemasks);
		} finally {
			if(f != null) {
				f.close();
			}
		}
		System.out.println(" Done.");
	}

	static void read_movegen() throws IOException {
		System.out.print("Reading movegen lookuptables...");
		ObjectInputStream f = null;
		try {
			f = new ObjectInputStream(new FileInputStream(Config.movegenFile));
			System.arraycopy(f.readObject(), 0, millmasks, 0, 1 << 24);
			System.arraycopy(f.readObject(), 0, untakemasks, 0, 1 << 24);
			System.arraycopy(f.readObject(), 0, millclosemasks, 0, 1 << 24);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Could not read movegen file.");
		} finally {
			if(f != null) {
				f.close();
			}
		}
		System.out.println(" Done.");
	}


	class Adj implements Serializable {
		private static final long serialVersionUID = 1L;

		public SectorId mozg, felrak, mozg_kle, felrak_kle;
	}

	Adj[][][][] adjs = new Adj[Config.maxPieceCount + 1][Config.maxPieceCount + 1][Config.maxPieceCount + 1][Config.maxPieceCount + 1];

	Movegen(Collection<SectorId> sectors, Collection<SectorId> mainSectors0) {
		HashSet<SectorId> mainSectors = new HashSet<>(mainSectors0);
		for(SectorId s: sectors) {
			byte w=s.w, b=s.b, wf=s.wf, bf=s.bf;
			Adj a = new Adj();
			SectorId
					mozg = new SectorId(b,w,bf,wf), //wms_w_b_wf_bf
					felrak = bf < Config.maxPieceCount && b > 0 ?
							new SectorId((byte)(b-1), w, (byte)(bf+1), wf) //wms_wm_b_wfp_bf
							: null,
					mozg_kle = w < Config.maxPieceCount ?
							new SectorId(b, (byte)(w+1), bf, wf) //wms_w_bp_wf_bf
							: null,
					felrak_kle = b > 0 && w < Config.maxPieceCount && bf < Config.maxPieceCount ?
							new SectorId((byte)(b-1), (byte)(w+1), (byte)(bf+1), wf) //wms_wm_bp_wfp_bf
							: null;
			if(mainSectors.contains(mozg)) a.mozg=mozg;
			if(mainSectors.contains(felrak)) a.felrak=felrak;
			if(mainSectors.contains(mozg_kle)) a.mozg_kle=mozg_kle;
			if(mainSectors.contains(felrak_kle)) a.felrak_kle=felrak_kle;
			adjs[s.w][s.b][s.wf][s.bf] = a;
		}
	}


	static int lssb(int x){return x&-x;} //returns mask of least significant set bit
	static long lssb(long x){return x&-x;}

	static long board_negate(long a){
		return (a>>24) | ((a&mask24)<<24);
	}

	public List<GameState> get_parents(long a, byte w, byte b, byte wf, byte bf){
		a = board_negate(a);

		Adj adj = adjs[w][b][wf][bf];

		byte tmp=w; w=b; b=tmp; //swap(w,b);
		tmp=wf; wf=bf; bf=tmp; //swap(wf,bf);

		List<GameState> parents = new ArrayList<GameState>();

		//mindig b kovetkezik, merthogy a fuggveny elejen negalunk (tehat a feher jatekos (mask24) lepeset csinaljuk visszafele)

		long free=(~(a|(a>>24)))&mask24;

		long cpmills = millmasks[(int)(a&mask24)]; //cp malomban levo korongjai
		long can_untake_mask = 0; //azok a poziciok, ahova rakhatunk vissza korongot
		if(cpmills != 0)
			can_untake_mask=(untakemasks[(int)(a>>24)]&free)<<24; //erre csak akkor lesz szukseg, ha van malom, amit esetleg kinyithatunk (itt meg lehetne gyorsitani, ha eltarolnank egy lookup table-ben azt is, hogy lehet-e malmot kinyitni (merthogy a blokkolasok miatt ez nem biztos jelenleg))

		long cp=a&mask24; //cp korongjai, kiveve ld. az alabbi sort  (a ciklus megeszi!)

		if(b+bf==Config.maxPieceCount)
			cp^=cpmills; //ha b+bf maxPieceCount fole menne, akkor nem csinalhatunk vissza malombecsukast

		////int[] adjmasks = w+wf>3 ? slide_adjmasks : fly_adjmasks; //javitottuk b-rol w-re  //(tovabba nem valtozik meg a lepesed soran)
		int[] adjmasks = slide_adjmasks;

		final boolean P_MOZG_COND = wf==0;
//		#if VARIANT==STANDARD || VARIANT==MORABARABA
//			//#define P_MOZG_COND (wf==0 && bf==0) //bf==0 was to avoid generating parents in non-existing sectors, but this is not really needed, and it would break FULL_SECTOR_GRAPH
//			#define P_MOZG_COND (wf==0)
//		#else //Lasker
//			#define	P_MOZG_COND true
//		#endif
//		//(P_FELRAK_COND azert nincs, mert ha visszacsinalunk egy felrakast, akkor lesz mit folrakni.)

		long kor; //az aktualis korong maskja
		while((kor=lssb(cp)) != 0){ cp^=kor;
			//unsigned long kor_i; _BitScanReverse64(&kor_i, kor); //az aktualis korong indexe
			int kor_i = Long.numberOfTrailingZeros(kor);
			long adjmask = adjmasks[kor_i] & free; //szabad szomszedos mezok  (a ciklus megeszi)
			long akor=a^kor;

			long moveso_mask; //move source mask
			if((cpmills&kor) == 0){ //ha nem malombol lepunk
				if(P_MOZG_COND){ //mozgatas
					if(adj.mozg != null) {
						while ((moveso_mask = lssb(adjmask)) != 0) {
							adjmask ^= moveso_mask;
							parents.add(new GameState(adj.mozg, akor ^ moveso_mask));
						}
					}
				}

				//felrakas
				if(adj.felrak != null) {
					parents.add(new GameState(adj.felrak, akor));
				}
			}else{ //ha malombol lepunk
				if(P_MOZG_COND){ //mozgatas
					if(adj.mozg_kle != null) {
						while ((moveso_mask = lssb(adjmask)) != 0) {
							adjmask ^= moveso_mask;
							long unt_mask; //untake mask
							long can_untake_mask0 = can_untake_mask & ~(moveso_mask << 24); //onnan sem vehetunk le korongot, ahonnan leptunk
							long akormoveso_mask = akor ^ moveso_mask;
							while ((unt_mask = lssb(can_untake_mask0)) != 0) {
								can_untake_mask0 ^= unt_mask;
								parents.add(new GameState(adj.mozg_kle, akormoveso_mask ^ unt_mask));
							}
						}
					}
				}

				//felrakas
				if(adj.felrak_kle != null) {
					long unt_mask; //untake mask
					long can_untake_mask0 = can_untake_mask; //mert a can_untake_mask-ot nem eheti meg a ciklus
					while ((unt_mask = lssb(can_untake_mask0)) != 0) {
						can_untake_mask0 ^= unt_mask;
						parents.add(new GameState(adj.felrak_kle, akor ^ unt_mask));
					}
				}
			}
		}

		return parents;
	}

	public List<GameState> get_parents(long a, SectorId id){
		return get_parents(a, id.w,id.b,id.wf,id.bf);
	}

	public List<GameState> get_parents(GameState a){
		return get_parents(a.board, a.sid);
	}



//void get_symparents(board a, int w, int b, int wf, int bf){
//	num_symparents=0;
//	get_parents(a,w,b,wf,bf);
//	for(int i=0; i<num_parents; i++){
//		/*for(int op=0; op<16; op++){
//			Parent &p=symparents[num_symparents++];
//			p=parents[i];
//			p.a=sym48(op,p.a);
//		}*/
//		Parent &p=parents[i];
//		int a=p.a&mask24;
//		char *iv=&invar0[invar[a]], n=iv[0];
//		for(int j=0; j<n; j++){
//			int op=iv[j+1];
//			Parent &sp=symparents[num_symparents++];
//			sp=parents[i];
//			sp.a=sym48(op,sp.a);
//		}
//	}
//}










//int std_child_count(board a, int w, int b, int wf, int bf){
//	
//	board free=(~(a|(a>>24)))&mask24;
//
//	int opp_notinmill=(int)(a>>24)^millmasks[a>>24]; //az ellenfel nem malomban levo korongjai
//	int opp_takeable_cnt=(opp_notinmill ?
//					__popcnt(opp_notinmill) :
//					__popcnt((unsigned int)(a>>24))); //barmelyik korongjat levehetjuk
//		
//	if(wf){ //felrakas
//		int mill_close=millclosemasks[a&mask24]; //malombezarasos poziciok
//		return  __popcnt(free & ~mill_close) +
//				__popcnt(free & mill_close) * opp_takeable_cnt;
//	}else{ //mozgatas
//		int r=0;
//		const board *adjmasks = w+wf>3 ? slide_adjmasks : fly_adjmasks;
//		board cp=a&mask24; //cp korongjai (a ciklus megeszi!)
//		board kor; //az aktualis korong maskja
//		while(kor=lssb(cp)){
//			cp^=kor;
//			unsigned long kor_i; _BitScanReverse64(&kor_i, kor); //az aktualis korong indexe
//			board adjmask = adjmasks[kor_i] & free; //szomszedos szabad mezok
//			
//			board akorm24=(a^kor)&mask24;
//			board moveend;
//			while(moveend=lssb(adjmask)){ adjmask^=moveend;
//				if(millmasks[akorm24^moveend]&moveend)
//					r+=opp_takeable_cnt;
//				else
//					r++;
//			}
//		}
//
//		return r;
//	}
//}


/*
	boolean can_close_mill(long a, int w, int b, int wf, int bf){
		if((a&(mask24<<24)) == 0) return false; //ha nincs az ellenfelnek korongja
		//Valojaban erre nem lenne szukseg, ld. doc-ban "Nincs korong f�nt probl�m�k" szekcio
		//	a Flink vilagban ez nincs atgondolva!

		long free=(~(a|(a>>24)))&mask24; //az also 24 biten mutatja, hogy se feher, se fekete nincs-e ott

	//	#if VARIANT==STANDARD || VARIANT==MORABARABA
	//		#define CCM_MOZG_COND (!CCM_FELRAK_COND)
	//		#define CCM_FELRAK_COND wf
	//	#else //Lasker
	//		#define CCM_MOZG_COND w
	//		#define CCM_FELRAK_COND wf
	//	#endif

		boolean CCM_FELRAK_COND = wf != 0;
		boolean CCM_MOZG_COND = !CCM_FELRAK_COND;

		if(CCM_FELRAK_COND){
			if((millclosemasks[(int)(a&mask24)] & free) != 0) //malombezarasos poziciok
				return true;
		}

		if(CCM_MOZG_COND){
			int[] adjmasks = w+wf>3 ? slide_adjmasks : fly_adjmasks;
			long cp=a&(mask24); //cp korongjai (a ciklus megeszi!)
			long kor; //az aktualis korong maskja
			while((kor=lssb(cp)) != 0){
				cp^=kor;
				//unsigned long kor_i; _BitScanReverse64(&kor_i, kor); //az aktualis korong indexe
				int kor_i = Long.numberOfTrailingZeros(kor);
				long adjmask = adjmasks[kor_i] & free; //szomszedos szabad mezok

				long akorm24=(a^kor)&mask24;
				long moveend;
				while((moveend=lssb(adjmask)) != 0){ adjmask^=moveend;
					if((millmasks[(int)(akorm24^moveend)]&moveend) != 0)
						return true;
				}
			}
		}

		return false;
	}

	public boolean can_close_mill(long a, SectorId id){
		return can_close_mill(a, id.w,id.b,id.wf,id.bf);
	}

	public boolean can_close_mill(GameState a){
		return can_close_mill(a.board, a.sid);
	}
*/



//unsigned int orbitsize(board a){
//	set<board> s;
//	for(int op=0; op<16; op++)
//		s.insert(sym48(op,a));
//	return s.size();
//}






//
//
//Child::Child(board a,Sector *s):a(a),s(s){}
//Child::Child(){}
//
//Child chd[1024]; //(uj variansoknal atgondolni (Std, Lasker, Morabaraba-ban atgondolva))
//int num_chd;
//id a_id;
//
//Sector *felrakas,*felrakas_kle,*mozg,*mozg_kle;
//
//void init_get_chd_sectors(id a_id0){
//	LOG("init_get_chd_sectors \n");
//	a_id=a_id0;
//
//	//meg vannak cserelve az indexek
//	felrakas = /*a_id.w<maxPieceCount &&*/ a_id.wf>0 ? sectors[a_id.b][a_id.w+1][a_id.bf][a_id.wf-1] : nullptr;
//	felrakas_kle = a_id.b>0 && /*a_id.w<maxPieceCount &&*/ a_id.wf>0 ? sectors[a_id.b-1][a_id.w+1][a_id.bf][a_id.wf-1] : nullptr;
//	mozg = sectors[a_id.b][a_id.w][a_id.bf][a_id.wf];
//	mozg_kle = a_id.b>0 ? sectors[a_id.b-1][a_id.w][a_id.bf][a_id.wf] : nullptr;
//}
//
//void get_chd(board a){
//	num_chd=0;
//
//	board free=(~(a|(a>>24)))&mask24; //a felrakasos agban megesszuk
//
//	board opp_notinmill=(a>>24)^millmasks[a>>24]; //az ellenfel nem malomban levo korongjai
//	board opp_takeable=opp_notinmill ?
//					(opp_notinmill<<24) :
//					(a&(mask24<<24)); //barmelyik korongjat levehetjuk
//
//	#if VARIANT==STANDARD || VARIANT==MORABARABA
//		#define C_MOZG_COND (!C_FELRAK_COND)
//		#define C_FELRAK_COND (a_id.wf)
//	#else //Lasker
//		#define C_MOZG_COND (a_id.w) //ez lehetne akar true is, mert ugyse menne bele a while-ba
//		#define C_FELRAK_COND (a_id.wf)
//	#endif
//
//	if(C_MOZG_COND){
//		const board *adjmasks = a_id.w+a_id.wf>3 ? slide_adjmasks : fly_adjmasks;
//		board cp=a&(mask24); //cp korongjai (a ciklus megeszi!)
//		board kor; //az aktualis korong maskja
//		while(kor=lssb(cp)){
//			cp^=kor;
//			unsigned long kor_i; _BitScanReverse64(&kor_i, kor); //az aktualis korong indexe
//			board adjmask = adjmasks[kor_i] & free; //szomszedos szabad mezok
//
//			board akorm24=(a^kor)&mask24;
//			board moveend;
//			while(moveend=lssb(adjmask)){ adjmask^=moveend;
//				if(millmasks[akorm24^moveend]&moveend){ //koronglevetel
//					board akormoveend=a^kor^moveend;
//					board opp_takeable0=opp_takeable; //megesszuk
//					board taking;
//					while(taking=lssb(opp_takeable0)){ opp_takeable0^=taking;
//						chd[num_chd++]=Child(akormoveend^taking, mozg_kle);
//					}
//				}else
//					chd[num_chd++]=Child(a^kor^moveend, mozg);
//			}
//		}
//	}
//
//	if(C_FELRAK_COND){
//		board kor;
//		while(kor=lssb(free)){ free^=kor;
//			board akor=a^kor;
//			if(millmasks[(akor)&mask24]&kor){ //koronglevetel
//				board opp_takeable0=opp_takeable; //megesszuk
//				board taking;
//				while(taking=lssb(opp_takeable0)){ opp_takeable0^=taking;
//					chd[num_chd++]=Child(akor^taking, felrakas_kle);
//				}
//			}else{
//				chd[num_chd++]=Child(akor, felrakas);
//			}
//		}
//	}
//
//	for(int i=0; i<num_chd; i++)
//		chd[i].a = board_negate(chd[i].a);
//}

}