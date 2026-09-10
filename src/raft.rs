// Implement the Raft core here, one step at a time.
use std::{cmp::min, iter::repeat_n};

mod types;
use crate::raft::types::{
    Error::{NotCandidate, NotLeader},
    Role::{Follower, Leader},
};

use self::types::*;

impl Raft {
    fn handle(&mut self, ev: Event) -> Result<Option<Effect>> {
        match ev {
            Event::Write => self.handle_write().map(Some),
            Event::LocalEntriesAppended(local_entry) => {
                self.handle_local_entry_appended(local_entry).map(Some)
            }
            Event::EntriesPersisted(entries) => self.handle_entries_persisted(entries),
            Event::EntriesApplied(entries) => self.handle_entries_applied(entries),
            Event::RequestVote(req) => self.handle_request_vote(req).map(Some),
            Event::AppendEntries(req) => self.handle_append_entries(req).map(Some),
            Event::VoteResponse(req) => self.handle_vote_response(req),
            Event::Heartbeat(request) => self.handle_heartbeat(request).map(Some),
            Event::HeartbeatResponse(response) => self.handle_heartbeat_response(response),
            Event::ElectionTimeout => self.handle_election_timeout().map(Some),
            Event::HeartbeatTimeout => self.handle_heartbeat_timeout().map(Some),
            Event::AppendEntriesResponse(response) => self.handle_append_entries_response(response),
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
            return Err(Error::NotLeader(self.leader_id.clone()));
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

    fn handle_heartbeat(&mut self, request: HeartbeatRequest) -> Result<Effect> {
        if request.term < self.current_term {
            return Ok(Effect::SendHeartbeatResponse {
                peer: request.leader_id,
                response: AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    last_log_index: self.last_log_index,
                    last_log_term: self.last_log_term,
                    leader_id: self.leader_id.clone(),
                },
                reset_election_timer: false,
                apply_through: None,
            });
        }

        if request.term > self.current_term {
            self.current_term = request.term;
            self.voted_for = None;
        }

        self.role = Role::Follower;
        self.current_votes = 0;
        self.leader_id = Some(request.leader_id.clone());

        let log_matches =
            request.log_index == self.last_log_index && request.log_term == self.last_log_term;

        if !log_matches {
            return Ok(Effect::SendHeartbeatResponse {
                peer: request.leader_id,
                response: AppendEntriesResponse {
                    term: self.current_term,
                    success: false,
                    last_log_index: self.last_log_index,
                    last_log_term: self.last_log_term,
                    leader_id: self.leader_id.clone(),
                },
                reset_election_timer: true,
                apply_through: None,
            });
        }

        let new_commit_index = min(request.leader_commit_index, self.last_log_index);
        let apply_through = if new_commit_index > self.commit_index {
            self.commit_index = new_commit_index;
            Some(new_commit_index)
        } else {
            None
        };

        Ok(Effect::SendHeartbeatResponse {
            peer: request.leader_id,
            response: AppendEntriesResponse {
                term: self.current_term,
                success: true,
                last_log_index: self.last_log_index,
                last_log_term: self.last_log_term,
                leader_id: self.leader_id.clone(),
            },
            reset_election_timer: true,
            apply_through,
        })
    }

    fn handle_append_entries(&mut self, append_request: AppendEntriesRequest) -> Result<Effect> {
        if append_request.term < self.current_term {
            return Ok(Effect::RejectAppendEntries {
                peer: append_request.leader_id,
                response: AppendEntriesResponse {
                    last_log_index: self.last_log_index,
                    last_log_term: self.last_log_term,
                    leader_id: self.leader_id.clone(),
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
                    leader_id: self.leader_id.clone(),
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
                return Ok(Effect::PersistEntries {
                    peer: append_request.leader_id,
                    response: AppendEntriesResponse {
                        last_log_index: append_request.prev_log_index,
                        last_log_term: append_request.prev_log_term,
                        leader_id: self.leader_id.clone(),
                        success: true,
                        term: self.current_term,
                    },
                    leader_commit_index: append_request.commit_index,
                    truncate_after: Some(append_request.prev_log_index),
                });
            }
        }

        self.current_term = append_request.term;
        self.role = Role::Follower;
        self.leader_id = Some(append_request.leader_id.clone());

        Ok(Effect::PersistEntries {
            peer: append_request.leader_id,
            response: AppendEntriesResponse {
                last_log_index: self.last_log_index,
                last_log_term: self.last_log_term,
                leader_id: self.leader_id.clone(),
                success: true,
                term: self.current_term,
            },
            leader_commit_index: append_request.commit_index,
            truncate_after: None,
        })
    }

    fn handle_entries_persisted(&mut self, entries: PersistedEntries) -> Result<Option<Effect>> {
        self.last_log_index = entries.last_log_index;
        self.last_log_term = entries.last_log_term;

        let new_commit_index = min(entries.leader_commit_index, self.last_log_index);
        if new_commit_index > self.commit_index {
            self.commit_index = new_commit_index;
            return Ok(Some(Effect::ApplyCommitted {
                through: self.commit_index,
            }));
        }
        Ok(None)
    }

    fn handle_entries_applied(&mut self, entries: AppliedEntries) -> Result<Option<Effect>> {
        self.last_applied = entries.through;
        if self.role == Role::Leader {
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
        Ok(None)
    }

    fn handle_local_entry_appended(&mut self, entry: LocalEntry) -> Result<Effect> {
        if self.role != Role::Leader {
            return Err(Error::NotLeader(self.leader_id.clone()));
        }

        let targets = self
            .voters
            .iter()
            .chain(self.learners.iter())
            .filter(|(node_id, _)| *node_id != &self.id)
            .map(|(node_id, progress)| {
                let last_log_index = min(progress.match_index, self.last_log_index);
                // to be filed by the executor.
                let last_log_term = self.last_log_term;

                ReplicationTarget {
                    peer: node_id.clone(),
                    last_log_index,
                    last_log_term,
                    leader_id: self.id.clone(),
                    leader_commit_index: self.commit_index,
                }
            })
            .collect();

        self.last_log_index = entry.index;
        self.last_log_term = entry.term;

        let leader_progress = self
            .voters
            .get_mut(&self.id)
            .expect("leader mut be present in voters");
        leader_progress.match_index = self.last_log_index;
        leader_progress.next_index = self.last_log_index + 1;

        Ok(Effect::SendAppendEntries { targets })
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

    fn handle_heartbeat_response(
        &mut self,
        received: ReceivedHeartbeatResponse,
    ) -> Result<Option<Effect>> {
        if received.response.term > self.current_term {
            self.current_term = received.response.term;
            self.role = Role::Follower;
            self.voted_for = None;
            self.leader_id = received.response.leader_id;
            self.current_votes = 0;
            return Ok(None);
        }

        if self.role != Role::Leader {
            return Err(Error::NotLeader(self.leader_id.clone()));
        }

        if received.response.term < self.current_term {
            return Ok(None);
        }

        let progress = self
            .voters
            .get_mut(&received.from)
            .or_else(|| self.learners.get_mut(&received.from));

        let Some(progress) = progress else {
            return Ok(None);
        };

        if received.response.success {
            let matched_index = min(received.response.last_log_index, self.last_log_index);
            progress.match_index = progress.match_index.max(matched_index);
            progress.next_index = progress
                .next_index
                .max(progress.match_index.saturating_add(1));
        } else {
            let previous_index = progress.next_index.saturating_sub(1);
            let follower_next_index = received.response.last_log_index.saturating_add(1);
            progress.next_index = min(previous_index, follower_next_index);
        }

        Ok(None)
    }

    fn handle_write(&self) -> Result<Effect> {
        if self.role != Role::Leader {
            return Err(Error::NotLeader(self.leader_id.clone()));
        }
        Ok(Effect::AppendLocal {
            term: self.current_term,
            last_log_index: self.last_log_index,
        })
    }

    fn handle_append_entries_response(
        &mut self,
        ReceivedAppendEntriesResponse { from, response }: ReceivedAppendEntriesResponse,
    ) -> Result<Option<Effect>> {
        if self.role != Leader {
            return Err(Error::NotLeader(response.leader_id));
        }

        if response.term > self.current_term {
            self.current_term = response.term;
            self.leader_id = response.leader_id.clone();
            self.role = Role::Follower;
            self.voted_for = None;
            return Err(Error::NotLeader(response.leader_id));
        }

        // if it's rejected.
        if !response.success {
            // what might be wrong.
            if response.term > self.current_term && response.leader_id != Some(self.id.clone()) {
                self.current_term = response.term;
                self.role = Role::Follower;
                self.voted_for = None;
                self.leader_id = self.leader_id.clone();
                self.current_votes = 0;
                return Err(Error::NotLeader(response.leader_id.clone()));
            }

            assert!(
                !(response.last_log_index > self.last_log_index),
                "follower last log index cannot be greater than leader's index"
            );

            if response.last_log_index < self.last_log_index {
                return Ok(Some(Effect::SendAppendEntries {
                    targets: vec![ReplicationTarget {
                        peer: from,
                        last_log_index: response.last_log_index,
                        last_log_term: response.last_log_term,
                        leader_id: self.id.clone(),
                        leader_commit_index: self.commit_index,
                    }],
                }));
            }
        }

        assert!(
            !(response.last_log_index > self.last_log_index),
            "follower's last log index cannot be greater than leader's index"
        );

        // is learner
        if let Some(mut learner_progress) = self.learners.remove(&from) {
            learner_progress.match_index = response.last_log_index;
            learner_progress.next_index = response.last_log_index + 1;

            if learner_progress.match_index < self.last_log_index {
                self.learners.insert(from.clone(), learner_progress);
                return Ok(Some(Effect::SendAppendEntries {
                    targets: vec![ReplicationTarget {
                        peer: from,
                        last_log_index: response.last_log_index,
                        last_log_term: response.last_log_term,
                        leader_id: self.id.clone(),
                        leader_commit_index: self.commit_index,
                    }],
                }));
            }
            // promote to voter;
            self.voters.insert(from, learner_progress);
            return Ok(None);
        }

        if let Some(mut peer_progress) = self.voters.remove(&from) {
            peer_progress.match_index = response.last_log_index;
            peer_progress.next_index = response.last_log_index + 1;

            if peer_progress.match_index < self.last_log_index {
                if self.last_log_index - peer_progress.match_index >= 10 {
                    self.learners.insert(from.clone(), peer_progress);
                } else {
                    self.voters.insert(from.clone(), peer_progress);
                }
                return Ok(Some(Effect::SendAppendEntries {
                    targets: vec![ReplicationTarget {
                        peer: from,
                        last_log_index: response.last_log_index,
                        last_log_term: response.last_log_term,
                        leader_id: self.id.clone(),
                        leader_commit_index: self.commit_index,
                    }],
                }));
            }

            // check if majority are up
            let mut caught_up: u64 = 1;

            if peer_progress.match_index == self.last_log_index {
                caught_up += 1;
            }

            for (node_id, voter_progress) in self.voters.iter() {
                if *node_id == self.id {
                    continue;
                }

                if voter_progress.match_index == self.last_log_index {
                    caught_up += 1;
                }
            }

            if caught_up > self.voters.len() as u64 / 2 {
                self.commit_index = self.last_log_index;
                return Ok(Some(Effect::ApplyCommitted {
                    through: self.commit_index,
                }));
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
    fn heartbeat_request_rejects_an_older_term_without_resetting_timer() {
        let mut raft = follower(4, None, 8, 3);

        let effect = raft
            .handle(Event::Heartbeat(HeartbeatRequest {
                term: 3,
                leader_id: node("node-2"),
                log_index: 8,
                log_term: 3,
                leader_commit_index: 6,
            }))
            .unwrap()
            .expect("heartbeat must produce a response");

        let Effect::SendHeartbeatResponse {
            peer,
            response,
            reset_election_timer,
            apply_through,
        } = effect
        else {
            panic!("heartbeat must produce a heartbeat response");
        };

        assert_eq!(peer, node("node-2"));
        assert_eq!(response.term, 4);
        assert_eq!(response.success, false);
        assert_eq!(reset_election_timer, false);
        assert_eq!(apply_through, None);
        assert_eq!(raft.current_term, 4);
        assert_eq!(raft.leader_id, Some(node("old-leader")));
    }

    #[test]
    fn valid_heartbeat_request_makes_candidate_step_down() {
        let mut raft = follower(4, Some(node("node-1")), 8, 3);
        raft.role = Role::Candidate;
        raft.current_votes = 2;

        let effect = raft
            .handle(Event::Heartbeat(HeartbeatRequest {
                term: 4,
                leader_id: node("node-2"),
                log_index: 8,
                log_term: 3,
                leader_commit_index: 0,
            }))
            .unwrap()
            .expect("heartbeat must produce a response");

        let Effect::SendHeartbeatResponse {
            response,
            reset_election_timer,
            ..
        } = effect
        else {
            panic!("heartbeat must produce a heartbeat response");
        };

        assert_eq!(response.success, true);
        assert_eq!(reset_election_timer, true);
        assert_eq!(raft.role, Role::Follower);
        assert_eq!(raft.leader_id, Some(node("node-2")));
        assert_eq!(raft.current_votes, 0);
    }

    #[test]
    fn heartbeat_request_with_higher_term_clears_previous_vote() {
        let mut raft = follower(4, Some(node("node-3")), 8, 3);

        raft.handle(Event::Heartbeat(HeartbeatRequest {
            term: 5,
            leader_id: node("node-2"),
            log_index: 8,
            log_term: 3,
            leader_commit_index: 0,
        }))
        .unwrap();

        assert_eq!(raft.current_term, 5);
        assert_eq!(raft.voted_for, None);
        assert_eq!(raft.role, Role::Follower);
        assert_eq!(raft.leader_id, Some(node("node-2")));
    }

    #[test]
    fn valid_heartbeat_advances_commit_only_through_local_log() {
        let mut raft = follower(4, None, 8, 3);
        raft.last_applied = 5;

        let effect = raft
            .handle(Event::Heartbeat(HeartbeatRequest {
                term: 4,
                leader_id: node("node-2"),
                log_index: 8,
                log_term: 3,
                leader_commit_index: 12,
            }))
            .unwrap()
            .expect("heartbeat must produce a response");

        let Effect::SendHeartbeatResponse {
            response,
            apply_through,
            ..
        } = effect
        else {
            panic!("heartbeat must produce a heartbeat response");
        };

        assert_eq!(response.success, true);
        assert_eq!(raft.commit_index, 8);
        assert_eq!(raft.last_applied, 5);
        assert_eq!(apply_through, Some(8));
    }

    #[test]
    fn heartbeat_with_mismatched_log_is_rejected_but_recognizes_leader() {
        let mut raft = follower(4, None, 8, 3);

        let effect = raft
            .handle(Event::Heartbeat(HeartbeatRequest {
                term: 5,
                leader_id: node("node-2"),
                log_index: 7,
                log_term: 2,
                leader_commit_index: 6,
            }))
            .unwrap()
            .expect("heartbeat must produce a response");

        let Effect::SendHeartbeatResponse {
            response,
            reset_election_timer,
            apply_through,
            ..
        } = effect
        else {
            panic!("heartbeat must produce a heartbeat response");
        };

        assert_eq!(response.success, false);
        assert_eq!(response.last_log_index, 8);
        assert_eq!(response.last_log_term, 3);
        assert_eq!(reset_election_timer, true);
        assert_eq!(apply_through, None);
        assert_eq!(raft.current_term, 5);
        assert_eq!(raft.leader_id, Some(node("node-2")));
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

        let Effect::PersistEntries {
            peer,
            response,
            truncate_after,
            leader_commit_index,
        } = effect
        else {
            panic!("a matching log prefix must be accepted");
        };

        assert_eq!(peer, node("node-2"));
        assert_eq!(response.term, 4);
        assert_eq!(response.success, true);
        assert_eq!(truncate_after, None);
        assert_eq!(leader_commit_index, 6);
        assert_eq!(raft.leader_id, Some(node("node-2")));
        assert_eq!(raft.role, Role::Follower);
    }

    #[test]
    fn append_entries_passes_leader_commit_index_to_persistence() {
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

        let Effect::PersistEntries {
            leader_commit_index,
            ..
        } = effect
        else {
            panic!("a matching log prefix must be accepted");
        };

        assert_eq!(leader_commit_index, 12);
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

        assert!(matches!(effect, Effect::PersistEntries { .. }));
        assert_eq!(raft.current_term, 5);
        assert_eq!(raft.role, Role::Follower);
        assert_eq!(raft.voted_for, None);
        assert_eq!(raft.leader_id, Some(node("node-2")));
    }

    #[test]
    fn append_entries_uses_persist_effect_when_truncation_is_required() {
        let mut raft = follower(4, None, 8, 3);

        let effect = raft
            .handle(Event::AppendEntries(AppendEntriesRequest {
                term: 5,
                leader_id: node("node-2"),
                prev_log_index: 5,
                prev_log_term: 2,
                commit_index: 4,
            }))
            .unwrap()
            .expect("accepted AppendEntries must produce an effect");

        let Effect::PersistEntries {
            response,
            truncate_after,
            leader_commit_index,
            ..
        } = effect
        else {
            panic!("accepted AppendEntries must be persisted");
        };

        assert_eq!(response.success, true);
        assert_eq!(truncate_after, Some(5));
        assert_eq!(leader_commit_index, 4);
    }

    #[test]
    fn entries_persisted_advances_commit_and_requests_application() {
        let mut raft = follower(4, None, 5, 2);
        raft.last_applied = 3;

        let effect = raft
            .handle(Event::EntriesPersisted(PersistedEntries {
                last_log_index: 8,
                last_log_term: 4,
                leader_commit_index: 6,
            }))
            .unwrap()
            .expect("a newly committed range must be applied");

        assert_eq!(raft.last_log_index, 8);
        assert_eq!(raft.last_log_term, 4);
        assert_eq!(raft.commit_index, 6);
        assert_eq!(raft.last_applied, 3);

        let Effect::ApplyCommitted { through } = effect else {
            panic!("persisting committed entries must request application");
        };

        assert_eq!(through, 6);
    }

    #[test]
    fn entries_applied_updates_last_applied() {
        let mut raft = follower(4, None, 5, 2);
        raft.last_applied = 3;

        let effect = raft
            .handle(Event::EntriesApplied(AppliedEntries { through: 6 }))
            .unwrap();

        assert!(effect.is_none());
        assert_eq!(raft.last_applied, 6);
    }

    #[test]
    fn entries_persisted_never_commits_beyond_the_local_log() {
        let mut raft = follower(4, None, 5, 2);

        let effect = raft
            .handle(Event::EntriesPersisted(PersistedEntries {
                last_log_index: 8,
                last_log_term: 4,
                leader_commit_index: 12,
            }))
            .unwrap()
            .expect("newly committed entries must be applied");

        assert_eq!(raft.commit_index, 8);

        let Effect::ApplyCommitted { through } = effect else {
            panic!("persisting committed entries must request application");
        };

        assert_eq!(through, 8);
    }

    #[test]
    fn write_on_leader_requests_a_local_append() {
        let mut raft = follower(4, Some(node("node-1")), 8, 3);
        raft.role = Role::Leader;

        let effect = raft
            .handle(Event::Write)
            .unwrap()
            .expect("a leader write must produce an effect");

        let Effect::AppendLocal {
            term,
            last_log_index,
        } = effect
        else {
            panic!("a leader write must append locally first");
        };

        assert_eq!(term, 4);
        assert_eq!(last_log_index, 8);
        assert_eq!(raft.last_log_index, 8);
    }

    #[test]
    fn write_on_follower_returns_known_leader() {
        let mut raft = follower(4, None, 8, 3);

        let result = raft.handle(Event::Write);

        assert!(matches!(
            result,
            Err(Error::NotLeader(Some(leader))) if leader == node("old-leader")
        ));
        assert_eq!(raft.last_log_index, 8);
    }

    #[test]
    fn local_entry_append_updates_leader_log_and_own_progress() {
        let local_node = node("node-1");
        let mut raft = follower(4, Some(local_node.clone()), 8, 3);
        raft.role = Role::Leader;

        raft.handle(Event::LocalEntriesAppended(LocalEntry {
            index: 9,
            term: 4,
        }))
        .unwrap();

        let leader_progress = raft.voters.get(&local_node).unwrap();
        assert_eq!(raft.last_log_index, 9);
        assert_eq!(raft.last_log_term, 4);
        assert_eq!(leader_progress.match_index, 9);
        assert_eq!(leader_progress.next_index, 10);
    }

    #[test]
    fn local_entry_append_creates_a_replication_target_for_every_other_node() {
        let local_node = node("node-1");
        let mut raft = follower(4, Some(local_node.clone()), 8, 3);
        raft.role = Role::Leader;
        raft.commit_index = 6;
        raft.voters.insert(
            node("node-2"),
            Progress {
                next_index: 6,
                match_index: 5,
            },
        );
        raft.voters.insert(
            node("node-3"),
            Progress {
                next_index: 9,
                match_index: 8,
            },
        );
        raft.learners.insert(
            node("node-4"),
            Progress {
                next_index: 3,
                match_index: 2,
            },
        );

        let effect = raft
            .handle(Event::LocalEntriesAppended(LocalEntry {
                index: 9,
                term: 4,
            }))
            .unwrap()
            .expect("a persisted local entry must be replicated");

        let Effect::SendAppendEntries { targets } = effect else {
            panic!("a persisted local entry must produce replication targets");
        };

        let targets: HashMap<_, _> = targets
            .into_iter()
            .map(|target| {
                (
                    target.peer,
                    (
                        target.last_log_index,
                        target.last_log_term,
                        target.leader_id,
                        target.leader_commit_index,
                    ),
                )
            })
            .collect();

        assert_eq!(
            targets,
            HashMap::from([
                (node("node-2"), (5, 3, local_node.clone(), 6)),
                (node("node-3"), (8, 3, local_node.clone(), 6)),
                (node("node-4"), (2, 3, local_node, 6)),
            ])
        );
    }

    #[test]
    fn local_entry_append_is_rejected_after_leader_steps_down() {
        let mut raft = follower(5, None, 8, 3);

        let result = raft.handle(Event::LocalEntriesAppended(LocalEntry {
            index: 9,
            term: 4,
        }));

        assert!(matches!(
            result,
            Err(Error::NotLeader(Some(leader))) if leader == node("old-leader")
        ));
        assert_eq!(raft.last_log_index, 8);
        assert_eq!(raft.last_log_term, 3);
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

    #[test]
    fn successful_heartbeat_response_updates_peer_progress() {
        let mut raft = follower(4, Some(node("node-1")), 8, 3);
        raft.role = Role::Leader;
        raft.voters.insert(
            node("node-2"),
            Progress {
                next_index: 6,
                match_index: 5,
            },
        );

        let effect = raft
            .handle(Event::HeartbeatResponse(ReceivedHeartbeatResponse {
                from: node("node-2"),
                response: AppendEntriesResponse {
                    term: 4,
                    success: true,
                    last_log_index: 8,
                    last_log_term: 3,
                    leader_id: Some(node("node-1")),
                },
            }))
            .unwrap();

        let progress = raft.voters.get(&node("node-2")).unwrap();
        assert!(effect.is_none());
        assert_eq!(progress.match_index, 8);
        assert_eq!(progress.next_index, 9);
    }

    #[test]
    fn rejected_heartbeat_response_rewinds_peer_next_index() {
        let mut raft = follower(4, Some(node("node-1")), 8, 3);
        raft.role = Role::Leader;
        raft.voters.insert(
            node("node-2"),
            Progress {
                next_index: 9,
                match_index: 0,
            },
        );

        let effect = raft
            .handle(Event::HeartbeatResponse(ReceivedHeartbeatResponse {
                from: node("node-2"),
                response: AppendEntriesResponse {
                    term: 4,
                    success: false,
                    last_log_index: 5,
                    last_log_term: 2,
                    leader_id: Some(node("node-1")),
                },
            }))
            .unwrap();

        let progress = raft.voters.get(&node("node-2")).unwrap();
        assert!(effect.is_none());
        assert_eq!(progress.match_index, 0);
        assert_eq!(progress.next_index, 6);
    }

    #[test]
    fn heartbeat_response_with_higher_term_makes_leader_step_down() {
        let mut raft = follower(4, Some(node("node-1")), 8, 3);
        raft.role = Role::Leader;
        raft.current_votes = 2;

        let effect = raft
            .handle(Event::HeartbeatResponse(ReceivedHeartbeatResponse {
                from: node("node-2"),
                response: AppendEntriesResponse {
                    term: 5,
                    success: false,
                    last_log_index: 8,
                    last_log_term: 3,
                    leader_id: Some(node("node-2")),
                },
            }))
            .unwrap();

        assert!(effect.is_none());
        assert_eq!(raft.current_term, 5);
        assert_eq!(raft.role, Role::Follower);
        assert_eq!(raft.voted_for, None);
        assert_eq!(raft.leader_id, Some(node("node-2")));
        assert_eq!(raft.current_votes, 0);
    }

    #[test]
    fn rejected_append_entries_response_retries_from_follower_position() {
        let local_node = node("node-1");
        let mut raft = follower(4, Some(local_node.clone()), 10, 4);
        raft.role = Role::Leader;
        raft.commit_index = 6;

        let effect = raft
            .handle(Event::AppendEntriesResponse(
                ReceivedAppendEntriesResponse {
                    from: node("node-2"),
                    response: AppendEntriesResponse {
                        term: 4,
                        success: false,
                        last_log_index: 5,
                        last_log_term: 2,
                        leader_id: Some(local_node.clone()),
                    },
                },
            ))
            .unwrap()
            .expect("a follower behind the leader must be retried");

        let Effect::SendAppendEntries { targets } = effect else {
            panic!("a rejected append must retry replication");
        };
        let [target] = targets.as_slice() else {
            panic!("only the rejecting follower should be retried");
        };

        assert_eq!(target.peer, node("node-2"));
        assert_eq!(target.last_log_index, 5);
        assert_eq!(target.last_log_term, 2);
        assert_eq!(target.leader_id, local_node);
        assert_eq!(target.leader_commit_index, 6);
    }

    #[test]
    fn learner_that_is_still_behind_remains_a_learner_and_is_retried() {
        let local_node = node("node-1");
        let learner = node("node-4");
        let mut raft = follower(4, Some(local_node.clone()), 10, 4);
        raft.role = Role::Leader;
        raft.learners.insert(learner.clone(), progress());

        let effect = raft
            .handle(Event::AppendEntriesResponse(
                ReceivedAppendEntriesResponse {
                    from: learner.clone(),
                    response: AppendEntriesResponse {
                        term: 4,
                        success: true,
                        last_log_index: 6,
                        last_log_term: 3,
                        leader_id: Some(local_node),
                    },
                },
            ))
            .unwrap()
            .expect("a learner that is behind must receive more entries");

        assert!(matches!(effect, Effect::SendAppendEntries { .. }));
        assert!(raft.learners.contains_key(&learner));
        assert!(!raft.voters.contains_key(&learner));

        let learner_progress = raft.learners.get(&learner).unwrap();
        assert_eq!(learner_progress.match_index, 6);
        assert_eq!(learner_progress.next_index, 7);
    }

    #[test]
    fn caught_up_learner_is_promoted_to_voter() {
        let local_node = node("node-1");
        let learner = node("node-4");
        let mut raft = follower(4, Some(local_node.clone()), 10, 4);
        raft.role = Role::Leader;
        raft.learners.insert(learner.clone(), progress());

        let effect = raft
            .handle(Event::AppendEntriesResponse(
                ReceivedAppendEntriesResponse {
                    from: learner.clone(),
                    response: AppendEntriesResponse {
                        term: 4,
                        success: true,
                        last_log_index: 10,
                        last_log_term: 4,
                        leader_id: Some(local_node),
                    },
                },
            ))
            .unwrap();

        assert!(effect.is_none());
        assert!(!raft.learners.contains_key(&learner));
        let voter_progress = raft
            .voters
            .get(&learner)
            .expect("caught-up learner must become a voter");
        assert_eq!(voter_progress.match_index, 10);
        assert_eq!(voter_progress.next_index, 11);
    }

    #[test]
    fn voter_that_is_still_behind_remains_in_voter_configuration() {
        let local_node = node("node-1");
        let voter = node("node-2");
        let mut raft = follower(4, Some(local_node.clone()), 10, 4);
        raft.role = Role::Leader;

        raft.handle(Event::AppendEntriesResponse(
            ReceivedAppendEntriesResponse {
                from: voter.clone(),
                response: AppendEntriesResponse {
                    term: 4,
                    success: true,
                    last_log_index: 7,
                    last_log_term: 3,
                    leader_id: Some(local_node),
                },
            },
        ))
        .unwrap();

        let voter_progress = raft
            .voters
            .get(&voter)
            .expect("a lagging voter must remain in the voter configuration");
        assert_eq!(voter_progress.match_index, 7);
        assert_eq!(voter_progress.next_index, 8);
        assert!(!raft.learners.contains_key(&voter));
    }

    #[test]
    fn successful_append_response_from_one_follower_forms_three_node_majority() {
        let local_node = node("node-1");
        let mut raft = follower(4, Some(local_node.clone()), 10, 4);
        raft.role = Role::Leader;
        raft.voters.insert(
            local_node.clone(),
            Progress {
                next_index: 11,
                match_index: 10,
            },
        );

        let effect = raft
            .handle(Event::AppendEntriesResponse(
                ReceivedAppendEntriesResponse {
                    from: node("node-2"),
                    response: AppendEntriesResponse {
                        term: 4,
                        success: true,
                        last_log_index: 10,
                        last_log_term: 4,
                        leader_id: Some(local_node),
                    },
                },
            ))
            .unwrap()
            .expect("leader plus one follower is a majority of three voters");

        assert_eq!(raft.commit_index, 10);
        let Effect::ApplyCommitted { through } = effect else {
            panic!("a majority-replicated entry must be applied");
        };
        assert_eq!(through, 10);
    }

    #[test]
    fn successful_append_response_with_higher_term_makes_leader_step_down() {
        let local_node = node("node-1");
        let new_leader = node("node-3");
        let mut raft = follower(4, Some(local_node), 10, 4);
        raft.role = Role::Leader;

        let result = raft.handle(Event::AppendEntriesResponse(
            ReceivedAppendEntriesResponse {
                from: node("node-2"),
                response: AppendEntriesResponse {
                    term: 5,
                    success: true,
                    last_log_index: 10,
                    last_log_term: 4,
                    leader_id: Some(new_leader.clone()),
                },
            },
        ));

        assert!(matches!(
            result,
            Err(Error::NotLeader(Some(leader))) if leader == new_leader
        ));
        assert_eq!(raft.current_term, 5);
        assert_eq!(raft.role, Role::Follower);
        assert_eq!(raft.voted_for, None);
        assert_eq!(raft.leader_id, Some(node("node-3")));
    }
}
