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

	if _, err := enrollNode(harness, "handover-east-01", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "handover-east-02", "east"); err != nil {
		return err
	}
	plan, revision, err := createActivePlan(harness, "handover.example", 5001, 2, 1, 100)
	if err != nil {
		return err
	}
	group, err := allocate(harness, plan.ID, "handover-lease-0001", "east", 2)
	if err != nil {
		return err
	}
	if len(group.LeaseIDs) != 2 {
		return ensure(false, "handover setup needs two claimed leases")
	}
	sourceLeaseID := group.LeaseIDs[0]
	peerLeaseID := group.LeaseIDs[1]

	receiver, err := enrollNode(harness, "handover-east-03", "east")
	if err != nil {
		return err
	}
	outside, err := enrollNode(harness, "handover-west-01", "west")
	if err != nil {
		return err
	}

	const reason = "shift change: operator rotation"
	result, err := handover(harness, sourceLeaseID, "handover-request-0001", receiver.ID, reason)
	if err != nil {
		return err
	}
	if result.SourceLease.ID != sourceLeaseID || result.SourceLease.State != domain.LeaseHandedOver {
		return ensure(false, "source lease did not become handed_over")
	}
	if result.SourceLease.HandoverToLeaseID != result.TargetLease.ID ||
		result.SourceLease.HandoverReason != reason ||
		result.SourceLease.FinishedAt == nil {
		return ensure(false, "source lease lost its handover lineage")
	}
	if result.TargetLease.State != domain.LeaseClaimed || result.TargetLease.NodeID != receiver.ID {
		return ensure(false, "target lease was not claimed by the receiver")
	}
	if result.TargetLease.HandoverFromLeaseID != sourceLeaseID ||
		result.TargetLease.HandoverReason != reason ||
		result.TargetLease.AllocationID != result.SourceLease.AllocationID {
		return ensure(false, "target lease lost its handover lineage")
	}
	if !result.TargetLease.ExpiresAt.Equal(result.SourceLease.ExpiresAt) ||
		!result.TargetLease.ClaimedAt.Equal(result.SourceLease.ClaimedAt) {
		return ensure(false, "handover did not preserve the original lease window")
	}
	if result.Handover.SourceLeaseID != sourceLeaseID ||
		result.Handover.TargetLeaseID != result.TargetLease.ID ||
		result.Handover.SourceNodeID != result.SourceLease.NodeID ||
		result.Handover.TargetNodeID != receiver.ID ||
		result.Handover.Reason != reason ||
		!result.Handover.ExpiresAt.Equal(result.SourceLease.ExpiresAt) {
		return ensure(false, "handover record does not explain the transfer")
	}

	var readyEast domain.NodePage
	if err := harness.call(http.MethodGet, "/v1/nodes?state=ready&region=east", nil, &readyEast); err != nil {
		return err
	}
	if !pageHasNode(readyEast, result.SourceLease.NodeID) {
		return ensure(false, "source node did not return to ready in the same fact")
	}
	var leasedEast domain.NodePage
	if err := harness.call(http.MethodGet, "/v1/nodes?state=leased&region=east", nil, &leasedEast); err != nil {
		return err
	}
	if !pageHasNode(leasedEast, receiver.ID) {
		return ensure(false, "receiving node did not become leased in the same fact")
	}

	_, err = submitProbe(harness, sourceLeaseID, "handover-probe-old", true, revision.Serial, 10)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseNotClaimed)); err != nil {
		return err
	}

	_, err = handover(harness, sourceLeaseID, "handover-request-0002", receiver.ID, reason)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	_, err = handover(harness, sourceLeaseID, "handover-request-0001", receiver.ID, reason)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	_, err = handover(harness, result.TargetLease.ID, "handover-request-0003", receiver.ID, reason)
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}
	_, err = handover(harness, peerLeaseID, "handover-request-0004", outside.ID, reason)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}
	_, err = handover(harness, peerLeaseID, "handover-request-0005", "nod_00000000000000000000", reason)
	if err := expectFailure(err, http.StatusNotFound, string(domain.CodeNotFound)); err != nil {
		return err
	}
	_, err = handover(harness, peerLeaseID, "handover-request-0006", receiver.ID, "")
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}
	_, err = handover(harness, peerLeaseID, "x", receiver.ID, reason)
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}
	_, err = handover(harness, peerLeaseID, "handover-request-0007", receiver.ID, reason)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}

	receiverProbe, err := submitProbe(harness, result.TargetLease.ID, "handover-probe-0001", true, revision.Serial, 12)
	if err != nil {
		return err
	}
	if receiverProbe.NodeID != receiver.ID {
		return ensure(false, "probe after handover was not attributed to the receiver")
	}
	peerProbe, err := submitProbe(harness, peerLeaseID, "handover-probe-0002", true, revision.Serial, 14)
	if err != nil {
		return err
	}
	if peerProbe.NodeID == receiver.ID || peerProbe.NodeID == result.SourceLease.NodeID {
		return ensure(false, "failed handovers disturbed the peer lease holder")
	}

	var assessment domain.RolloutAssessment
	if err := harness.call(http.MethodPatch, "/v1/plans/"+plan.ID+"/assessments", domain.AssessmentInput{
		RequestKey: "handover-assess-0001",
	}, &assessment); err != nil {
		return err
	}
	if assessment.Verdict != domain.VerdictConsistent || assessment.ProbeCount != 2 || assessment.SuccessCount != 2 {
		return ensure(false, "assessment did not cover probes from both sides of the handover")
	}
	var latest domain.RolloutAssessment
	if err := harness.call(http.MethodGet, "/v1/plans/"+plan.ID+"/assessments/latest", nil, &latest); err != nil {
		return err
	}
	if latest.ID != assessment.ID {
		return ensure(false, "latest assessment does not match the post-handover verdict")
	}

	secondPlan, _, err := createActivePlan(harness, "handover2.example", 6001, 1, 1, 100)
	if err != nil {
		return err
	}
	secondGroup, err := allocate(harness, secondPlan.ID, "handover-lease-0002", "east", 1)
	if err != nil {
		return err
	}
	freshLeaseID := secondGroup.LeaseIDs[0]
	var current domain.RolloutPlan
	if err := harness.call(http.MethodGet, "/v1/plans/"+secondPlan.ID, nil, &current); err != nil {
		return err
	}
	if err := harness.call(http.MethodPatch, "/v1/plans/"+secondPlan.ID+"/complete", map[string]any{
		"expected_version": current.Version,
	}, &current); err != nil {
		return err
	}
	_, err = handover(harness, freshLeaseID, "handover-request-0008", receiver.ID, reason)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeInvalidState)); err != nil {
		return err
	}
	harness.clock.Advance(601 * time.Second)
	_, err = handover(harness, freshLeaseID, "handover-request-0009", receiver.ID, reason)
	return expectFailure(err, http.StatusConflict, string(domain.CodeLeaseExpired))
}
