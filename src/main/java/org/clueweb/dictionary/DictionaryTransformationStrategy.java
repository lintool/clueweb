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
 */

package org.clueweb.dictionary;

import it.unimi.dsi.bits.TransformationStrategies;
import it.unimi.dsi.bits.TransformationStrategy;

import java.nio.charset.CharacterCodingException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

public class DictionaryTransformationStrategy {
	public static TransformationStrategy<CharSequence> getStrategy() {
		return TransformationStrategies.prefixFreeUtf16();
	}

	public static class WritableComparator extends
			org.apache.hadoop.io.WritableComparator {
		private final TransformationStrategy<CharSequence> strategy = DictionaryTransformationStrategy
				.getStrategy();

		public WritableComparator() {
			super(Text.class);
		}

		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int n1 = WritableUtils.decodeVIntSize(b1[s1]);
			int n2 = WritableUtils.decodeVIntSize(b2[s2]);

			String t1 = null, t2 = null;
			try {
				t1 = Text.decode(b1, s1 + n1, l1 - n1);
				t2 = Text.decode(b2, s2 + n2, l2 - n2);
			} catch (CharacterCodingException e) {
				throw new RuntimeException(e);
			}

			return strategy.toBitVector(t1).compareTo(strategy.toBitVector(t2));
		}
	}
}
