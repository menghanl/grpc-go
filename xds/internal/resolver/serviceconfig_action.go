/*
 *
 * Copyright 2020 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package resolver

import (
	"fmt"
	"sort"
	"strconv"

	xdsclient "google.golang.org/grpc/xds/internal/client"
)

type action struct {
	clustersWithWeights map[string]uint32 // cluster:weight, "A":40, "B":60
	clusterNames        string            // clusters without weights, sorted, "A_B_"

	assignedName string // the assigned name, clusters plus index number, "A_B_1"
}

// type actionsWithNextIndex struct {
// 	actions   map[string]action // key is clusters+weights, "A20_B30_"
// 	nextIndex uint64
// }

// oldActions contains the final actions. The following code updates
// oldActions with updates from RDS resp.
// var oldActions map[string]actionsWithNextIndex
// var oldActions map[string]map[string]action

// Turn an RDS response into a map of actions.
// newActions contains new actions, with names unassigned
//
// Note that this only handles actions, but not matches. The routes in the
// generated balancer config should maintain the same order as in RDS resp.
// A separate list of routes would work.

// build a map of weighted clusters
// from
// {B:60, A:40}, {A:30, B:70}, {B:90, C:10}
//
// to
//   A40_B60_: {{A:40, B:60}, "A_B_", ""}
//   A30_B70_: {{A:30, B:70}, "A_B_", ""}
//   B90_C10_: {{B:90, C:10}, "B_C_", ""}
func newActionsFromRoutes(routes []*xdsclient.Route) map[string]action {
	newActions := make(map[string]action)

	for _, route := range routes {
		var clusterNames []string
		clusters := route.Action
		for n := range clusters {
			clusterNames = append(clusterNames, n)
		}

		// Sort names to be consistent.
		sort.Strings(clusterNames)
		clustersOnly := ""
		clustersWithWeight := ""
		for _, c := range clusterNames {
			// Generates A_B_
			clustersOnly = clustersOnly + c + "_"
			// Generates A40_B60_
			clustersWithWeight = clustersWithWeight + c + strconv.FormatUint(uint64(clusters[c]), 10) + "_"
		}

		if _, ok := newActions[clustersWithWeight]; !ok {
			newActions[clustersWithWeight] = action{
				clustersWithWeights: clusters,
				clusterNames:        clustersOnly,
			}
		}
	}
	return newActions
}

// Take an old and a new map. In the old map, all actions have field name set.
// In the new map, all actions have no name.
//
// The returned map has all actions from the new map, with names set to
// - if the action exists in old, get the old name
// - if the action doesn't exist in old
//   - if there is a to-be-removed action with the same set of clusters, reuse
//     the name
//   - otherwise, generate a new name
func (r *xdsResolver) updateActions(newActions map[string]action) {
	// Delete actions from oldActions if they are not in newActions.
	// Keep the removed actions in a map, with key as clusterNames without
	// weights, so their assigned names can be reused.
	if r.actions == nil {
		r.actions = make(map[string]action)
	}
	oldActions := r.actions
	actionsRemoved := make(map[string]action)
	for clusterNamesWithWeight, act := range oldActions {
		if _, ok := newActions[clusterNamesWithWeight]; !ok {
			actionsRemoved[act.clusterNames] = act
			delete(oldActions, clusterNamesWithWeight)
		}
	}

	// Find actions in newActions but not in oldActions. Add them, and try to
	// reuse assigned names from actionsRemoved.
	if r.nextIndex == nil {
		r.nextIndex = make(map[string]int)
	}
	nextIndex := r.nextIndex
	for clusterNamesWithWeight, act := range newActions {
		if _, ok := oldActions[clusterNamesWithWeight]; !ok {
			if actt, ok := actionsRemoved[act.clusterNames]; ok {
				// Reuse name from actionsRemoved.
				act.assignedName = actt.assignedName
				delete(actionsRemoved, act.clusterNames)
				oldActions[clusterNamesWithWeight] = act
				continue
			}
			// Generate a new name.
			idx := nextIndex[act.clusterNames]
			nextIndex[act.clusterNames]++
			act.assignedName = fmt.Sprintf("%s%d", act.clusterNames, idx)
			oldActions[clusterNamesWithWeight] = act
		}
	}

	// Delete entry from nextIndex if all actions with the clusters are removed.
	remainingClusterNames := make(map[string]bool)
	for _, act := range oldActions {
		remainingClusterNames[act.clusterNames] = true
	}
	for clusterNames := range nextIndex {
		if !remainingClusterNames[clusterNames] {
			delete(nextIndex, clusterNames)
		}
	}
}
