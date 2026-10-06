package workflowprobe

import (
	"net/http"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runLeaseHandover() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-handover-01", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-handover-02", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-handover-03", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-handover-04", "west"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "handover.example", 7001, 2, 0.5, 100)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "handover-lease-0001", "east", 2)
	if err != nil {
		return err
	}
	if len(group.LeaseIDs) != 2 {
		return ensure(false, "handover setup needs two leases")
	}

	var readyBefore domain.NodePage
	if err := harness.call(http.MethodGet, "/v1/nodes?region=east&state=ready", nil, &readyBefore); err != nil {
		return err
	}
	if len(readyBefore.Items) != 1 {
		return ensure(false, "exactly one east node must remain ready before handover")
	}
	successor := readyBefore.Items[0]

	var planBefore domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &planBefore); err != nil {
		return err
	}

	handed, err := handover(harness, group.LeaseIDs[0], "shift-change-east-rotation")
	if err != nil {
		return err
	}
	if handed.Handover.PreviousLeaseID != group.LeaseIDs[0] ||
		handed.Handover.OriginLeaseID != group.LeaseIDs[0] ||
		handed.Handover.Sequence != 1 {
		return ensure(false, "handover lineage does not start at the original lease")
	}
	if handed.Handover.Reason != "shift-change-east-rotation" {
		return ensure(false, "handover reason was not preserved")
	}
	if handed.Handover.FromNodeID != handed.FromLease.NodeID || handed.Handover.ToNodeID != successor.ID {
		return ensure(false, "handover fact does not name the original and successor nodes")
	}
	if handed.FromLease.State != domain.LeaseHandedOver ||
		handed.FromLease.SupersededByLeaseID != handed.ToLease.ID ||
		handed.FromLease.FinishedAt == nil {
		return ensure(false, "original lease was not closed as handed_over")
	}
	if handed.ToLease.State != domain.LeaseClaimed || handed.ToLease.NodeID != successor.ID {
		return ensure(false, "successor lease is not claimed by the receiving node")
	}
	if !handed.ToLease.ExpiresAt.Equal(handed.FromLease.ExpiresAt) {
		return ensure(false, "remaining deadline was not preserved across handover")
	}
	if handed.ToLease.AllocationID != handed.FromLease.AllocationID {
		return ensure(false, "successor lease lost the original allocation lineage")
	}
	if handed.ToLease.Handover == nil ||
		handed.ToLease.Handover.OriginLeaseID != group.LeaseIDs[0] ||
		!handed.ToLease.ClaimedAt.Equal(handed.Handover.At) {
		return ensure(false, "successor lease does not carry the handover lineage")
	}

	var planAfter domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &planAfter); err != nil {
		return err
	}
	if planAfter.Version != planBefore.Version+1 {
		return ensure(false, "plan version did not switch in the same fact as the handover")
	}
	eastLeased, err := nodeIDSet(harness, "east", domain.NodeLeased)
	if err != nil {
		return err
	}
	eastReady, err := nodeIDSet(harness, "east", domain.NodeReady)
	if err != nil {
		return err
	}
	if !eastLeased[successor.ID] || !eastReady[handed.Handover.FromNodeID] {
		return ensure(false, "node states did not switch with the handover")
	}

	_, err = submitProbe(harness, group.LeaseIDs[0], "handover-probe-0001", true, revision.Serial, 10)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseNotClaimed)); err != nil {
		return err
	}
	probe, err := submitProbe(harness, handed.ToLease.ID, "handover-probe-0002", true, revision.Serial, 10)
	if err != nil {
		return err
	}
	if probe.NodeID != successor.ID || probe.LeaseID != handed.ToLease.ID {
		return ensure(false, "probe after handover is not bound to the receiving node")
	}

	_, err = handover(harness, group.LeaseIDs[0], "shift-change-east-again")
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	_, err = handover(harness, handed.ToLease.ID, "shift-change-east-third")
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}

	westGroup, err := allocate(harness, plan.ID, "handover-lease-0002", "west", 1)
	if err != nil {
		return err
	}
	_, err = handover(harness, westGroup.LeaseIDs[0], "shift-change-west-rotation")
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}

	var planUnchanged domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID, nil, &planUnchanged); err != nil {
		return err
	}
	if planUnchanged.Version != planAfter.Version {
		return ensure(false, "failed handovers left partial state behind")
	}

	var assessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "handover-assess-0001",
	}, &assessment); err != nil {
		return err
	}
	if assessment.ProbeCount != 1 || assessment.Verdict != domain.VerdictReviewRequired ||
		!containsReason(assessment.Reasons, "node_not_observed") {
		return ensure(false, "assessment does not reflect the post-handover probe source")
	}

	harness.clock.Advance(601 * time.Second)
	_, err = handover(harness, group.LeaseIDs[1], "shift-change-east-expired")
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseExpired)); err != nil {
		return err
	}
	return nil
}

func handover(harness *harness, leaseID string, reason string) (domain.HandoverResult, error) {
	var result domain.HandoverResult
	err := harness.call(http.MethodPatch, "/v1/leases/"+leaseID+"/handover", domain.HandoverInput{
		Reason: reason,
	}, &result)
	return result, err
}

func nodeIDSet(harness *harness, region string, state domain.NodeState) (map[string]bool, error) {
	var page domain.NodePage
	path := "/v1/nodes?region=" + region + "&state=" + string(state)
	if err := harness.call(http.MethodGet, path, nil, &page); err != nil {
		return nil, err
	}
	ids := make(map[string]bool, len(page.Items))
	for _, node := range page.Items {
		ids[node.ID] = true
	}
	return ids, nil
}

func containsReason(reasons []string, wanted string) bool {
	for _, reason := range reasons {
		if reason == wanted {
			return true
		}
	}
	return false
}
