package org.apache.flink.streaming.api;

import java.util.LinkedList;

public class TestCaseMaster {

	static long[] numTuples={100L,1000L}; //,10000L,100000L,1000000L,10000000L};
	static int[] testCases={1,2,3};
	static long windowSize=50;
	static long slideSize=21;
	
	public static void main(String[] args) throws Exception {
		for (int testCase:testCases){
			
			LinkedList<Long> results=new LinkedList<Long>();
			
			for (long numTuple:numTuples){
				
				results.add(TestCase1.runTest(numTuple, windowSize, slideSize, testCase));
				
			}
			
			System.out.println("**********************************");
			System.out.println("TEST SERIES "+testCase);
			System.out.println("**********************************");
			System.out.println("");
			for (int i=0;i<results.size();i++){
				System.out.println(results.get(i)+"\t"+numTuples[i]);
			}
			
		}
	}
	
}
