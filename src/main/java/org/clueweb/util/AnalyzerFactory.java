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
 * @author: Claudia Hauff
 * 
 * Class that is responsible for providing the correct Lucene Analyzer.
 * So far used by LMRetrieval and ComputeTermStatistics.
 */

package org.clueweb.util;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;
import org.clueweb.dictionary.PorterAnalyzer;

public class AnalyzerFactory {

	public static Analyzer getAnalyzer(String analyzerType)
	{
		if(analyzerType.equals("standard")) {
			return new org.apache.lucene.analysis.standard.StandardAnalyzer(Version.LUCENE_43);
		}
		
		if(analyzerType.equals("porter")) {
			return new PorterAnalyzer();
		}
		
		
		return null;
	}
	
	public static String getOptions()
	{
		return "standard|porter";
	}
}
