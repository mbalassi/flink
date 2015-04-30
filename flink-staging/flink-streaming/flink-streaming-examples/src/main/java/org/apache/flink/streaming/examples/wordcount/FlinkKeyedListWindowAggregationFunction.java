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

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class FlinkKeyedListWindowAggregationFunction implements WindowMapFunction<Tuple2<String, Integer>, Tuple2<String, Iterable<Integer>>> {

    @Override
    public void mapWindow(Iterable<Tuple2<String, Integer>> values, Collector<Tuple2<String, Iterable<Integer>>> out) throws Exception {
        Iterator<Tuple2<String, Integer>> it = values.iterator();
        Tuple2<String, Integer> first = it.next();
        Iterable<Integer> passThrough = new PassThroughIterable(first, it);;
        out.collect(new Tuple2<String, Iterable<Integer>>(first.f0, passThrough));
    }

    private static class PassThroughIterable implements Iterable<Integer>, Iterator<Integer> {
        private Tuple2<String, Integer> first;
        private Iterator<Tuple2<String, Integer>> iterator;

        public PassThroughIterable(Tuple2<String, Integer> first, Iterator<Tuple2<String, Integer>> iterator) {
            this.first = first;
            this.iterator = iterator;
        }

        @Override
        public Iterator<Integer> iterator() {
            return this;
        }

        @Override
        public boolean hasNext() {
            return first != null || iterator.hasNext();
        }

        @Override
        public Integer next() {
            if (first != null) {
                Integer result = first.f1;
                first = null;
                return result;
            } else {
                return iterator.next().f1;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove elements from input.");
        }
    }
}
