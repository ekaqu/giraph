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
package org.apache.giraph.lib;

import org.apache.giraph.graph.BasicVertex;
import org.apache.giraph.graph.VertexWriter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * OutputFormat to write out the graph nodes as text, value-separated (by
 * tabs, by default).  With the default delimiter, a vertex is written out as:
 *
 * <VertexId><tab><Vertex Value><tab>[<EdgeId><tab><EdgeValue>]+
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class AdjacencyListTextVertexOutputFormat <I extends WritableComparable,
    V extends Writable, E extends Writable> extends TextVertexOutputFormat<I, V, E>{

  static class AdjacencyListVertexWriter<I extends WritableComparable, V extends
      Writable, E extends Writable> extends TextVertexWriter<I, V, E> {
    public static final String LINE_TOKENIZE_VALUE = "adj.list.output.delimiter";
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    private String delimiter;

    public AdjacencyListVertexWriter(RecordWriter<Text,Text> recordWriter) {
      super(recordWriter);
    }

    @Override
    public void writeVertex(BasicVertex<I, V, E, ?> vertex) throws IOException,
        InterruptedException {
      if (delimiter == null) {
        delimiter = getContext().getConfiguration()
           .get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
      }

      StringBuffer sb = new StringBuffer(vertex.getVertexId().toString());
      sb.append(delimiter);
      sb.append(vertex.getVertexValue().toString());

      for (I edge : vertex) {
        sb.append(delimiter).append(edge);
        sb.append(delimiter).append(vertex.getEdgeValue(edge));
      }

      getRecordWriter().write(new Text(sb.toString()), null);
    }
  }

  @Override
  public VertexWriter<I, V, E> createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new AdjacencyListVertexWriter<I, V, E>
        (textOutputFormat.getRecordWriter(context));
  }

}
