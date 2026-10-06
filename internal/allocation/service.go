package allocation

import (
	"context"
	"sort"
	"strings"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type Service struct {
	store *store.Store
	clock platform.Clock
	ids   platform.IDGenerator
}

func New(stateStore *store.Store, clock platform.Clock, ids platform.IDGenerator) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids}
}

func (s *Service) Allocate(
	ctx context.Context,
	planID string,
	input domain.LeaseInput,
) (domain.LeaseGroup, error) {
	if err := domain.ValidateLeaseInput(input); err != nil {
		return domain.LeaseGroup{}, err
	}
	input.Region = strings.TrimSpace(strings.ToLower(input.Region))
	requestIndex := planID + "\x00" + input.RequestKey
	fingerprint := domain.LeaseFingerprint(planID, input)
	now := s.clock.Now().UTC()
	var group domain.LeaseGroup

	err := s.store.Write(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.State != domain.PlanActive {
			return domain.InvalidState("plan must be active for node leasing", map[string]any{
				"plan_id": planID,
				"state":   plan.State,
			})
		}
		if existingID, exists := state.LeaseRequestKeys[requestIndex]; exists {
			existing, found := state.LeaseGroups[existingID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "lease request index is inconsistent", map[string]any{
					"lease_group_id": existingID,
				})
			}
			if existing.RequestFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different lease parameters", map[string]any{
					"request_key": input.RequestKey,
				})
			}
			group = existing
			return nil
		}

		expireLeases(state, now)
		candidates := make([]domain.ResolverNode, 0)
		for _, node := range state.Nodes {
			if node.State != domain.NodeReady {
				continue
			}
			if input.Region != "" && node.Region != input.Region {
				continue
			}
			candidates = append(candidates, node)
		}
		if len(candidates) == 0 {
			return domain.NewError(domain.CodeNodeUnavailable, "no eligible node is ready", map[string]any{
				"plan_id": planID,
				"region":  input.Region,
			})
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].Region != candidates[j].Region {
				return candidates[i].Region < candidates[j].Region
			}
			return candidates[i].ID < candidates[j].ID
		})
		if len(candidates) > input.MaxNodes {
			candidates = candidates[:input.MaxNodes]
		}

		groupID := s.ids.New("grp")
		leaseIDs := make([]string, 0, len(candidates))
		expiresAt := now.Add(time.Duration(input.LeaseSeconds) * time.Second)
		for _, candidate := range candidates {
			leaseID := s.ids.New("lse")
			candidate.State = domain.NodeLeased
			candidate.Version++
			candidate.UpdatedAt = now
			state.Nodes[candidate.ID] = candidate
			state.Leases[leaseID] = domain.NodeLease{
				ID:           leaseID,
				PlanID:       planID,
				NodeID:       candidate.ID,
				AllocationID: groupID,
				State:        domain.LeaseClaimed,
				ClaimedAt:    now,
				ExpiresAt:    expiresAt,
			}
			leaseIDs = append(leaseIDs, leaseID)
		}
		group = domain.LeaseGroup{
			ID:                 groupID,
			PlanID:             planID,
			RequestKey:         input.RequestKey,
			RequestFingerprint: fingerprint,
			Region:             input.Region,
			LeaseIDs:           leaseIDs,
			CreatedAt:          now,
		}
		state.LeaseGroups[group.ID] = group
		state.LeaseRequestKeys[requestIndex] = group.ID
		store.AddAudit(state, s.ids, "lease_group", group.ID, "created", now, map[string]any{
			"plan_id":     planID,
			"lease_count": len(leaseIDs),
		})
		return nil
	})
	return group, err
}

func (s *Service) Release(ctx context.Context, leaseID string) (domain.NodeLease, error) {
	now := s.clock.Now().UTC()
	var result domain.NodeLease
	err := s.store.Write(ctx, func(state *domain.State) error {
		lease, ok := state.Leases[leaseID]
		if !ok {
			return domain.NotFound("lease", leaseID)
		}
		if lease.State == domain.LeaseReleased || lease.State == domain.LeaseExpired {
			result = lease
			return nil
		}
		if lease.State != domain.LeaseClaimed {
			return domain.InvalidState("only a claimed lease can be released", map[string]any{
				"lease_id": leaseID,
				"state":    lease.State,
			})
		}
		node, ok := state.Nodes[lease.NodeID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
				"node_id": lease.NodeID,
			})
		}
		finishedAt := now
		lease.State = domain.LeaseReleased
		lease.FinishedAt = &finishedAt
		state.Leases[leaseID] = lease
		if node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
		store.AddAudit(state, s.ids, "lease", leaseID, "released", now, nil)
		result = lease
		return nil
	})
	return result, err
}

func (s *Service) Handover(
	ctx context.Context,
	leaseID string,
	input domain.HandoverInput,
) (domain.LeaseHandoverResult, error) {
	if err := domain.ValidateHandoverInput(input); err != nil {
		return domain.LeaseHandoverResult{}, err
	}
	input.RequestKey = strings.TrimSpace(input.RequestKey)
	input.TargetNodeID = strings.TrimSpace(input.TargetNodeID)
	input.Reason = strings.TrimSpace(input.Reason)
	now := s.clock.Now().UTC()
	var result domain.LeaseHandoverResult
	err := s.store.Write(ctx, func(state *domain.State) error {
		source, ok := state.Leases[leaseID]
		if !ok {
			return domain.NotFound("lease", leaseID)
		}
		if source.State != domain.LeaseClaimed {
			return domain.InvalidState("only a claimed lease can be handed over", map[string]any{
				"lease_id": leaseID,
				"state":    source.State,
			})
		}
		if !now.Before(source.ExpiresAt) {
			return domain.NewError(domain.CodeLeaseExpired, "lease has expired", map[string]any{
				"lease_id":   leaseID,
				"expires_at": source.ExpiresAt,
			})
		}
		plan, ok := state.Plans[source.PlanID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing plan", map[string]any{
				"plan_id": source.PlanID,
			})
		}
		if plan.State != domain.PlanActive {
			return domain.InvalidState("plan must be active for lease handover", map[string]any{
				"plan_id": plan.ID,
				"state":   plan.State,
			})
		}
		sourceNode, ok := state.Nodes[source.NodeID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
				"node_id": source.NodeID,
			})
		}
		targetNode, ok := state.Nodes[input.TargetNodeID]
		if !ok {
			return domain.NotFound("node", input.TargetNodeID)
		}
		if targetNode.ID == sourceNode.ID {
			return domain.Validation("target node must differ from the current lease holder", map[string]any{
				"node_id": targetNode.ID,
			})
		}
		if targetNode.Region != sourceNode.Region {
			return domain.NewError(domain.CodeNodeUnavailable, "target node is outside the lease region", map[string]any{
				"node_id":      targetNode.ID,
				"node_region":  targetNode.Region,
				"lease_region": sourceNode.Region,
			})
		}
		if targetNode.State != domain.NodeReady {
			return domain.NewError(domain.CodeNodeUnavailable, "target node is not ready", map[string]any{
				"node_id": targetNode.ID,
				"state":   targetNode.State,
			})
		}

		target := domain.NodeLease{
			ID:                  s.ids.New("lse"),
			PlanID:              source.PlanID,
			NodeID:              targetNode.ID,
			AllocationID:        source.AllocationID,
			State:               domain.LeaseClaimed,
			ClaimedAt:           source.ClaimedAt,
			ExpiresAt:           source.ExpiresAt,
			HandoverFromLeaseID: source.ID,
			HandoverReason:      input.Reason,
		}
		finishedAt := now
		source.State = domain.LeaseHandedOver
		source.FinishedAt = &finishedAt
		source.HandoverToLeaseID = target.ID
		source.HandoverReason = input.Reason
		state.Leases[source.ID] = source
		state.Leases[target.ID] = target

		if sourceNode.State == domain.NodeLeased {
			sourceNode.State = domain.NodeReady
			sourceNode.Version++
			sourceNode.UpdatedAt = now
			state.Nodes[sourceNode.ID] = sourceNode
		}
		targetNode.State = domain.NodeLeased
		targetNode.Version++
		targetNode.UpdatedAt = now
		state.Nodes[targetNode.ID] = targetNode

		handover := domain.LeaseHandover{
			ID:            s.ids.New("hov"),
			PlanID:        source.PlanID,
			SourceLeaseID: source.ID,
			TargetLeaseID: target.ID,
			SourceNodeID:  sourceNode.ID,
			TargetNodeID:  targetNode.ID,
			RequestKey:    input.RequestKey,
			Reason:        input.Reason,
			ExpiresAt:     source.ExpiresAt,
			CreatedAt:     now,
		}
		state.Handovers[handover.ID] = handover
		store.AddAudit(state, s.ids, "lease_handover", handover.ID, "created", now, map[string]any{
			"plan_id":         source.PlanID,
			"source_lease_id": source.ID,
			"target_lease_id": target.ID,
			"source_node_id":  sourceNode.ID,
			"target_node_id":  targetNode.ID,
			"reason":          input.Reason,
		})
		result = domain.LeaseHandoverResult{
			Handover:    handover,
			SourceLease: source,
			TargetLease: target,
		}
		return nil
	})
	return result, err
}

func (s *Service) ExpireDue(ctx context.Context) (int, error) {
	now := s.clock.Now().UTC()
	expired := 0
	err := s.store.Write(ctx, func(state *domain.State) error {
		expired = expireLeases(state, now)
		return nil
	})
	return expired, err
}

func expireLeases(state *domain.State, now time.Time) int {
	count := 0
	for leaseID, lease := range state.Leases {
		if lease.State != domain.LeaseClaimed || lease.ExpiresAt.After(now) {
			continue
		}
		finishedAt := now
		lease.State = domain.LeaseExpired
		lease.FinishedAt = &finishedAt
		state.Leases[leaseID] = lease
		if node, ok := state.Nodes[lease.NodeID]; ok && node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
		count++
	}
	return count
}
