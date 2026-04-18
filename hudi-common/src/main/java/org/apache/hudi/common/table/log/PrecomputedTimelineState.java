/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.table.timeline.HoodieTimeline;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Precomputed timeline state passed from query planning side to task side.
 */
public class PrecomputedTimelineState implements Serializable {

  private final String completedTimelineStartInstant;
  private final Set<String> completedInstants;
  private final Set<String> inflightInstants;

  public PrecomputedTimelineState(String completedTimelineStartInstant, Set<String> completedInstants, Set<String> inflightInstants) {
    this.completedTimelineStartInstant = completedTimelineStartInstant;
    this.completedInstants = Collections.unmodifiableSet(new HashSet<>(completedInstants));
    this.inflightInstants = Collections.unmodifiableSet(new HashSet<>(inflightInstants));
  }

  public boolean containsCompletedInstantOrBeforeTimelineStarts(String instantTime) {
    if (completedInstants.contains(instantTime)) {
      return true;
    }
    return completedTimelineStartInstant != null
        && HoodieTimeline.compareTimestamps(instantTime, HoodieTimeline.LESSER_THAN_OR_EQUALS, completedTimelineStartInstant);
  }

  public boolean containsInflightInstant(String instantTime) {
    return inflightInstants.contains(instantTime);
  }
}
