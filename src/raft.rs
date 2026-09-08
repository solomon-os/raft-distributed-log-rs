// Implement the Raft core here, one step at a time.
use std::cmp::min;

mod types;
use crate::raft::types::Error::NotCandidate;

use self::types::*;

impl Raft {
    fn handle(&mut self, ev: Event) -> Result<Option<Effect>> {
        match ev {
            Event::ElectionTimeout => self.handle_election_timeout().map(Some),
            Event::RequestVote(req) => self.handle_request_vote(req).map(Some),
            Event::HeartbeatTimeout => self.handle_heartbeat_timeout().map(Some),
            Event::AppendEntries(req) => self.handle_append_entries(req).map(Some),
            Event::AppendProcessed(req) => {
                self.handle_append_processed(req)?;
                Ok(None)
            }
            Event::VoteResponse(req) => self.handle_vote_response(req),
        }
    }

    fn handle_election_timeout(&mut self) -> Result<Effect> {
        self.current_term += 1;
        self.current_votes = 1;
        self.voted_for = Some(self.id.clone());

        let peers = self
            .voters
            .keys()
            .filter(|node_id| *node_id != &self.id)
            .cloned()
            .collect();

        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone()); // vote for my self;
        Ok(Effect::SendRequestVotes {
            peers,
            request: VoteRequest {
                term: self.current_term,
                candidate_id: self.id.clone(),
                last_log_index: self.last_log_index,
                last_log_term: self.last_log_term,
            },
        })
    }

    fn handle_request_vote(&mut self, vote_request: VoteRequest) -> Result<Effect> {
        if vote_request.term < self.current_term || vote_request.last_log_term < self.last_log_term
        {
            return Ok(Effect::SendRequestVoteResponse {
                peer: vote_request.candidate_id,
                response: VoteResponse {
                    term: self.current_term,
                    vote_granted: false,
                },
            });
        }

        if vote_request.last_log_term == self.last_log_term
            && (vote_request.last_log_index < self.last_log_index)
        {
            return Ok(Effect::SendRequestVoteResponse {
                peer: vote_request.candidate_id,
                response: VoteResponse {
                    term: self.current_term,
                    vote_granted: false,
                },
            });
        }

        if vote_request.term == self.current_term
            && self
                .voted_for
                .as_ref()
                .is_some_and(|node| *node != vote_request.candidate_id)
        {
            return Ok(Effect::SendRequestVoteResponse {
                peer: vote_request.candidate_id,
                response: VoteResponse {
                    term: self.current_term,
                    vote_granted: false,
                },
            });
        }

        self.voted_for = Some(vote_request.candidate_id.clone());
        self.role = Role::Follower;
        self.current_term = vote_request.term;

        Ok(Effect::SendRequestVoteResponse {
            peer: vote_request.candidate_id,
            response: VoteResponse {
                term: self.current_term,
                vote_granted: true,
            },
        })
    }

    fn handle_heartbeat_timeout(&self) -> Result<Effect> {
        if self.role != Role::Leader {
            return Err(Error::NotLeader);
        }
        Ok(Effect::SendHeartbeat {
            peers: self
                .voters
                .keys()
                .filter(|node_id| *node_id != &self.id)
                .cloned()
                .collect(),
            request: HeartbeatRequest {
                term: self.current_term,
                leader_id: self.id.clone(),
                log_index: self.last_log_index,
                log_term: self.last_log_term,
                leader_commit_index: self.commit_index,
            },
        })
    }

    fn handle_append_entries(&mut self, append_request: AppendEntriesRequest) -> Result<Effect> {
        if append_request.term < self.current_term {
            return Ok(Effect::RejectAppendEntries {
                peer: append_request.leader_id,
                response: AppendEntriesResponse {
                    last_log_index: self.last_log_index,
                    last_log_term: self.last_log_term,
                    success: false,
                    term: self.current_term,
                },
            });
        }

        if append_request.prev_log_term == self.last_log_term
            && append_request.prev_log_index > self.last_log_index
        {
            self.current_term = append_request.term;
            self.leader_id = Some(append_request.leader_id.clone());
            return Ok(Effect::RejectAppendEntries {
                peer: append_request.leader_id.clone(),
                response: AppendEntriesResponse {
                    last_log_index: self.last_log_index,
                    last_log_term: self.last_log_term,
                    success: false,
                    term: self.current_term,
                },
            });
        }

        if append_request.term > self.current_term {
            self.voted_for = None;
            if append_request.prev_log_index < self.last_log_index {
                self.role = Role::Follower;
                self.current_term = append_request.term;
                self.leader_id = Some(append_request.leader_id.clone());
                return Ok(Effect::TruncateAppendEntries {
                    peer: append_request.leader_id,
                    response: AppendEntriesResponse {
                        last_log_index: append_request.prev_log_index,
                        last_log_term: append_request.prev_log_term,
                        success: true,
                        term: self.current_term,
                    },
                });
            }
        }

        self.current_term = append_request.term;
        self.role = Role::Follower;
        self.leader_id = Some(append_request.leader_id.clone());

        Ok(Effect::ProcessAppendEntries {
            peer: append_request.leader_id,
            response: AppendEntriesResponse {
                last_log_index: self.last_log_index,
                last_log_term: self.last_log_term,
                success: true,
                term: self.current_term,
            },
            apply_through: min(self.last_log_index, append_request.commit_index),
        })
    }

    fn handle_append_processed(&mut self, append_processed: AppendProcessed) -> Result<()> {
        if let Some(applied_through) = append_processed.applied_through {
            self.last_applied = applied_through;
        }
        self.last_log_index = append_processed.last_log_index;
        Ok(())
    }

    fn handle_vote_response(
        &mut self,
        vote_response: ReceivedVoteResponse,
    ) -> Result<Option<Effect>> {
        if self.role != Role::Candidate {
            return Err(NotCandidate);
        }

        if vote_response.response.vote_granted {
            self.current_votes += 1;
            if self.role != Role::Leader {
                if self.current_votes > self.voters.len() as u64 / 2 {
                    // make leader and return heartbeat;
                    self.role = Role::Leader;
                    return Ok(Some(Effect::SendHeartbeat {
                        peers: self
                            .voters
                            .keys()
                            .filter(|node_id| *node_id != &self.id)
                            .cloned()
                            .collect(),
                        request: HeartbeatRequest {
                            term: self.current_term,
                            leader_id: self.id.clone(),
                            log_index: self.last_log_index,
                            log_term: self.last_log_term,
                            leader_commit_index: self.commit_index,
                        },
                    }));
                }
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use std::collections::{HashMap, HashSet};

    fn node(id: &str) -> NodeId {
        NodeId(id.to_owned())
    }

    fn progress() -> Progress {
        Progress {
            next_index: 0,
            match_index: 0,
        }
    }

    fn follower(
        current_term: u64,
        voted_for: Option<NodeId>,
        last_log_index: u64,
        last_log_term: u64,
    ) -> Raft {
        let local_node = node("node-1");

        Raft {
            id: local_node.clone(),
            voters: HashMap::from([
                (local_node, progress()),
                (node("node-2"), progress()),
                (node("node-3"), progress()),
            ]),
            learners: HashMap::new(),
            role: Role::Follower,
            current_term,
            voted_for,
            last_applied: 0,
            commit_index: 0,
            last_log_index,
            last_log_term,
            leader_id: Some(node("old-leader")),
            current_votes: 0,
        }
    }

    fn request_vote(
        raft: &mut Raft,
        candidate_id: NodeId,
        term: u64,
        last_log_index: u64,
        last_log_term: u64,
    ) -> (NodeId, u64, bool) {
        let effect = raft
            .handle(Event::RequestVote(VoteRequest {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            }))
            .unwrap()
            .expect("RequestVote must produce an effect");

        match effect {
            Effect::SendRequestVoteResponse { peer, response } => {
                (peer, response.term, response.vote_granted)
            }
            Effect::SendRequestVotes { .. } => panic!("RequestVote must produce a response"),
            _ => panic!("RequestVote must produce a vote response"),
        }
    }

    #[test]
    fn election_timeout_starts_a_new_election() {
        let local_node = node("node-1");
        let mut raft = Raft {
            id: local_node.clone(),
            voters: HashMap::from([
                (local_node.clone(), progress()),
                (node("node-2"), progress()),
                (node("node-3"), progress()),
            ]),
            learners: HashMap::new(),
            role: Role::Follower,
            current_term: 2,
            voted_for: None,
            last_applied: 0,
            commit_index: 0,
            last_log_index: 8,
            last_log_term: 2,
            leader_id: Some(node("old-leader")),
            current_votes: 0,
        };

        let effect = raft
            .handle(Event::ElectionTimeout)
            .unwrap()
            .expect("ElectionTimeout must produce an effect");

        assert_eq!(raft.role, Role::Candidate);
        assert_eq!(raft.current_term, 3);
        assert_eq!(raft.voted_for, Some(local_node.clone()));

        let Effect::SendRequestVotes { peers, request } = effect else {
            panic!("ElectionTimeout must produce vote requests");
        };

        assert_eq!(request.term, 3);
        assert_eq!(request.candidate_id, local_node);
        assert_eq!(request.last_log_index, 8);
        assert_eq!(request.last_log_term, 2);

        let requested_votes: HashSet<NodeId> = peers.into_iter().collect();

        assert_eq!(
            requested_votes,
            HashSet::from([node("node-2"), node("node-3")])
        );
    }

    #[test]
    fn election_requests_votes_only_from_other_voters() {
        let local_node = node("node-1");
        let mut raft = Raft {
            id: local_node.clone(),
            voters: HashMap::from([
                (local_node.clone(), progress()),
                (node("node-2"), progress()),
            ]),
            learners: HashMap::from([(node("node-3"), progress())]),
            role: Role::Follower,
            current_term: 2,
            voted_for: None,
            last_applied: 0,
            commit_index: 0,
            last_log_index: 8,
            last_log_term: 2,
            leader_id: Some(node("old-leader")),
            current_votes: 0,
        };

        let effect = raft
            .handle(Event::ElectionTimeout)
            .unwrap()
            .expect("ElectionTimeout must produce an effect");

        let Effect::SendRequestVotes { peers, .. } = effect else {
            panic!("ElectionTimeout must produce vote requests");
        };
        let requested_votes: HashSet<NodeId> = peers.into_iter().collect();

        assert_eq!(requested_votes, HashSet::from([node("node-2")]));
    }

    #[test]
    fn request_vote_rejects_an_older_term() {
        let mut raft = follower(4, None, 8, 3);

        let response = request_vote(&mut raft, node("node-2"), 3, 8, 3);

        assert_eq!(response, (node("node-2"), 4, false));
        assert_eq!(raft.current_term, 4);
        assert_eq!(raft.voted_for, None);
        assert_eq!(raft.role, Role::Follower);
    }

    #[test]
    fn request_vote_with_a_newer_term_makes_candidate_step_down() {
        let local_node = node("node-1");
        let mut raft = follower(4, Some(local_node), 8, 3);
        raft.role = Role::Candidate;

        let response = request_vote(&mut raft, node("node-2"), 5, 8, 3);

        assert_eq!(response, (node("node-2"), 5, true));
        assert_eq!(raft.current_term, 5);
        assert_eq!(raft.voted_for, Some(node("node-2")));
        assert_eq!(raft.role, Role::Follower);
    }

    #[test]
    fn request_vote_rejects_a_second_candidate_in_the_same_term() {
        let mut raft = follower(4, Some(node("node-3")), 8, 3);

        let response = request_vote(&mut raft, node("node-2"), 4, 8, 3);

        assert_eq!(response, (node("node-2"), 4, false));
        assert_eq!(raft.voted_for, Some(node("node-3")));
    }

    #[test]
    fn request_vote_is_idempotent_for_the_same_candidate() {
        let mut raft = follower(4, Some(node("node-2")), 8, 3);

        let response = request_vote(&mut raft, node("node-2"), 4, 8, 3);

        assert_eq!(response, (node("node-2"), 4, true));
        assert_eq!(raft.voted_for, Some(node("node-2")));
    }

    #[test]
    fn request_vote_rejects_a_candidate_with_a_stale_log() {
        let mut stale_term = follower(4, None, 8, 3);
        let mut stale_index = follower(4, None, 8, 3);

        let stale_term_response = request_vote(&mut stale_term, node("node-2"), 4, 100, 2);
        let stale_index_response = request_vote(&mut stale_index, node("node-2"), 4, 7, 3);

        assert_eq!(stale_term_response, (node("node-2"), 4, false));
        assert_eq!(stale_index_response, (node("node-2"), 4, false));
    }

    #[test]
    fn request_vote_grants_a_candidate_with_an_up_to_date_log() {
        let mut newer_term = follower(4, None, 8, 3);
        let mut same_position = follower(4, None, 8, 3);

        let newer_term_response = request_vote(&mut newer_term, node("node-2"), 4, 1, 4);
        let same_position_response = request_vote(&mut same_position, node("node-2"), 4, 8, 3);

        assert_eq!(newer_term_response, (node("node-2"), 4, true));
        assert_eq!(same_position_response, (node("node-2"), 4, true));
    }

    #[test]
    fn append_entries_rejects_an_older_leader_term() {
        let mut raft = follower(4, None, 8, 3);

        let effect = raft
            .handle(Event::AppendEntries(AppendEntriesRequest {
                term: 3,
                leader_id: node("node-2"),
                prev_log_index: 8,
                prev_log_term: 3,
                commit_index: 6,
            }))
            .unwrap()
            .expect("AppendEntries must produce an effect");

        let Effect::RejectAppendEntries { peer, response } = effect else {
            panic!("an older leader term must be rejected");
        };

        assert_eq!(peer, node("node-2"));
        assert_eq!(response.term, 4);
        assert_eq!(response.success, false);
        assert_eq!(response.last_log_index, 8);
        assert_eq!(response.last_log_term, 3);
        assert_eq!(raft.current_term, 4);
        assert_eq!(raft.leader_id, Some(node("old-leader")));
    }

    #[test]
    fn append_entries_rejects_a_missing_previous_log_entry() {
        let mut raft = follower(4, None, 8, 3);

        let effect = raft
            .handle(Event::AppendEntries(AppendEntriesRequest {
                term: 4,
                leader_id: node("node-2"),
                prev_log_index: 9,
                prev_log_term: 3,
                commit_index: 6,
            }))
            .unwrap()
            .expect("AppendEntries must produce an effect");

        let Effect::RejectAppendEntries { peer, response } = effect else {
            panic!("a missing previous log entry must be rejected");
        };

        assert_eq!(peer, node("node-2"));
        assert_eq!(response.term, 4);
        assert_eq!(response.success, false);
        assert_eq!(response.last_log_index, 8);
        assert_eq!(response.last_log_term, 3);
    }

    #[test]
    fn append_entries_accepts_a_matching_log_prefix() {
        let mut raft = follower(4, None, 8, 3);

        let effect = raft
            .handle(Event::AppendEntries(AppendEntriesRequest {
                term: 4,
                leader_id: node("node-2"),
                prev_log_index: 8,
                prev_log_term: 3,
                commit_index: 6,
            }))
            .unwrap()
            .expect("AppendEntries must produce an effect");

        let Effect::ProcessAppendEntries {
            peer,
            response,
            apply_through,
        } = effect
        else {
            panic!("a matching log prefix must be accepted");
        };

        assert_eq!(peer, node("node-2"));
        assert_eq!(response.term, 4);
        assert_eq!(response.success, true);
        assert_eq!(apply_through, 6);
        assert_eq!(raft.leader_id, Some(node("node-2")));
        assert_eq!(raft.role, Role::Follower);
    }

    #[test]
    fn append_entries_never_applies_beyond_the_local_log() {
        let mut raft = follower(4, None, 8, 3);

        let effect = raft
            .handle(Event::AppendEntries(AppendEntriesRequest {
                term: 4,
                leader_id: node("node-2"),
                prev_log_index: 8,
                prev_log_term: 3,
                commit_index: 12,
            }))
            .unwrap()
            .expect("AppendEntries must produce an effect");

        let Effect::ProcessAppendEntries { apply_through, .. } = effect else {
            panic!("a matching log prefix must be accepted");
        };

        assert_eq!(apply_through, 8);
    }

    #[test]
    fn append_entries_from_a_newer_term_makes_a_candidate_step_down() {
        let local_node = node("node-1");
        let mut raft = follower(4, Some(local_node), 8, 3);
        raft.role = Role::Candidate;

        let effect = raft
            .handle(Event::AppendEntries(AppendEntriesRequest {
                term: 5,
                leader_id: node("node-2"),
                prev_log_index: 8,
                prev_log_term: 3,
                commit_index: 6,
            }))
            .unwrap()
            .expect("AppendEntries must produce an effect");

        assert!(matches!(effect, Effect::ProcessAppendEntries { .. }));
        assert_eq!(raft.current_term, 5);
        assert_eq!(raft.role, Role::Follower);
        assert_eq!(raft.voted_for, None);
        assert_eq!(raft.leader_id, Some(node("node-2")));
    }

    #[test]
    fn append_processed_updates_log_and_applied_indexes() {
        let mut raft = follower(4, None, 5, 2);
        raft.last_applied = 3;

        let effect = raft
            .handle(Event::AppendProcessed(AppendProcessed {
                last_log_index: 8,
                applied_through: Some(6),
            }))
            .unwrap();

        assert!(effect.is_none());
        assert_eq!(raft.last_log_index, 8);
        assert_eq!(raft.last_applied, 6);
    }

    #[test]
    fn append_processed_without_application_preserves_last_applied() {
        let mut raft = follower(4, None, 5, 2);
        raft.last_applied = 3;

        let effect = raft
            .handle(Event::AppendProcessed(AppendProcessed {
                last_log_index: 8,
                applied_through: None,
            }))
            .unwrap();

        assert!(effect.is_none());
        assert_eq!(raft.last_log_index, 8);
        assert_eq!(raft.last_applied, 3);
    }

    #[test]
    fn granted_vote_that_reaches_a_majority_makes_candidate_leader() {
        let local_node = node("node-1");
        let mut raft = follower(4, Some(local_node.clone()), 8, 3);
        raft.role = Role::Candidate;
        raft.current_votes = 1;

        let effect = raft
            .handle(Event::VoteResponse(ReceivedVoteResponse {
                from: node("node-2"),
                response: VoteResponse {
                    term: 4,
                    vote_granted: true,
                },
            }))
            .unwrap();

        assert_eq!(raft.current_votes, 2);
        assert_eq!(raft.role, Role::Leader);

        let effect = effect.expect("a newly elected leader must send an initial heartbeat");

        let Effect::SendHeartbeat { peers, request } = effect else {
            panic!("winning an election must send an initial heartbeat");
        };

        assert_eq!(request.term, 4);
        assert_eq!(request.leader_id, local_node);
        assert_eq!(
            peers.into_iter().collect::<HashSet<_>>(),
            HashSet::from([node("node-2"), node("node-3")])
        );
    }

    #[test]
    fn denied_vote_does_not_change_candidate_votes() {
        let local_node = node("node-1");
        let mut raft = follower(4, Some(local_node), 8, 3);
        raft.role = Role::Candidate;
        raft.current_votes = 1;

        let effect = raft
            .handle(Event::VoteResponse(ReceivedVoteResponse {
                from: node("node-2"),
                response: VoteResponse {
                    term: 4,
                    vote_granted: false,
                },
            }))
            .unwrap();

        assert!(effect.is_none());
        assert_eq!(raft.current_votes, 1);
        assert_eq!(raft.role, Role::Candidate);
    }

    #[test]
    fn vote_response_is_rejected_when_node_is_not_a_candidate() {
        let mut raft = follower(4, None, 8, 3);

        let result = raft.handle(Event::VoteResponse(ReceivedVoteResponse {
            from: node("node-2"),
            response: VoteResponse {
                term: 4,
                vote_granted: true,
            },
        }));

        assert!(matches!(result, Err(Error::NotCandidate)));
        assert_eq!(raft.current_votes, 0);
        assert_eq!(raft.role, Role::Follower);
    }
}
