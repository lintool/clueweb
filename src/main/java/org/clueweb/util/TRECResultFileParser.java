package org.clueweb.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.Maps;

/*
 * ClueWeb Tools: Hadoop tools for manipulating ClueWeb collections

 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 * 
 * @author Claudia Hauff
 */

public class TRECResultFileParser {

	private BufferedReader br;
	private FSDataInputStream fsin;
	private String line;
	private boolean hasNext;
	
	public boolean hasNext() throws IOException {
		if( (line=br.readLine())!=null) {
			hasNext = true;
		}
		else {
			hasNext = false;
		}
		return hasNext;
	}
	
	public TRECResult getNext() {
		if(!hasNext) {
			return null;
		}
		
		String tokens[] = line.split("\\s+");
		return new TRECResult(Integer.parseInt(tokens[0]), tokens[2], Integer.parseInt(tokens[3]), Double.parseDouble(tokens[4]), line);
	}
	
	public TRECResultFileParser(FileSystem fs, Path p) throws IOException {

		// read the TREC result file of the initial retrieval run as input
		fsin = fs.open(p);
		br = new BufferedReader(new InputStreamReader(fsin));
	}
	
	//TODO: check name!!
	public void finalize() throws IOException
	{
		br.close();
		fsin.close();
	}
}
