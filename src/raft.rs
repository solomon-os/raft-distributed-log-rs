// Implement the Raft core here, one step at a time.

use std::collections::{HashMap, HashSet};

use serf::net::Node;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(String);

#[derive(Debug, PartialEq, Eq)]
pub enum Role {
    Candidate,
    Follower,
    Leader,
}

pub enum Event {
    ElectionTimeout,
}

pub struct RequestVote {
    term: u64,
    candidate_id: NodeId,
    last_log_index: u64,
    last_log_term: u64,
}

pub enum Effect {
    SendRequestVote { peer: NodeId, request: RequestVote },
}

struct Raft {
    id: NodeId,
    voters: HashMap<NodeId, Progress>,
    learners: HashMap<NodeId, Progress>,
    role: Role,
    current_term: u64,
    voted_for: Option<NodeId>,
    last_applied: u64,
    commit_index: u64,
    last_log_index: u64,
    last_log_term: u64,
}

struct Progress {
    next_index: u64,
    match_index: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("node is not the leader")]
    NotLeader,
}

type Result<T> = std::result::Result<T, Error>;

impl Raft {
    fn handle(&mut self, ev: Event) -> Result<Vec<Effect>> {
        match ev {
            Event::ElectionTimeout => self.handle_election_timeout(),
        }
    }

    fn handle_election_timeout(&mut self) -> Result<Vec<Effect>> {
        let mut effects = Vec::with_capacity(self.voters.len());

        self.current_term += 1;

        for node_id in self.voters.keys() {
            if node_id == &self.id {
                continue;
            }
            effects.push(Effect::SendRequestVote {
                peer: node_id.clone(),
                request: RequestVote {
                    term: self.current_term,
                    candidate_id: self.id.clone(),
                    last_log_index: self.last_log_index,
                    last_log_term: self.last_log_term,
                },
            });
        }

        self.role = Role::Candidate;
        self.voted_for = Some(self.id.clone()); // vote for my self;
        Ok(effects)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    fn node(id: &str) -> NodeId {
        NodeId(id.to_owned())
    }

    fn progress() -> Progress {
        Progress {
            next_index: 0,
            match_index: 0,
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
        };

        let effects = raft.handle(Event::ElectionTimeout).unwrap();

        assert_eq!(raft.role, Role::Candidate);
        assert_eq!(raft.current_term, 3);
        assert_eq!(raft.voted_for, Some(local_node.clone()));

        let mut requested_votes = HashSet::new();

        for effect in effects {
            let Effect::SendRequestVote { peer, request } = effect;

            requested_votes.insert(peer);
            assert_eq!(request.term, 3);
            assert_eq!(request.candidate_id, local_node);
            assert_eq!(request.last_log_index, 8);
            assert_eq!(request.last_log_term, 2);
        }

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
        };

        let effects = raft.handle(Event::ElectionTimeout).unwrap();

        let requested_votes: HashSet<NodeId> = effects
            .into_iter()
            .map(|effect| match effect {
                Effect::SendRequestVote { peer, .. } => peer,
            })
            .collect();

        assert_eq!(requested_votes, HashSet::from([node("node-2")]));
    }
}
