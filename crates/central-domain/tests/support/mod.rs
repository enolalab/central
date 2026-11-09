use central_domain::{
    decide, Actor, ActorId, ActorKind, Command, PlanDocumentV1, PlanTaskV1, RunId, RunState,
    TaskId, TaskKind, Timestamp, VerifySpec,
};

pub fn now() -> Timestamp {
    Timestamp(1_700_000_000_000)
}

pub fn later() -> Timestamp {
    Timestamp(1_700_000_060_000)
}

pub fn user() -> Actor {
    Actor {
        id: ActorId::from_u128(100),
        kind: ActorKind::User,
    }
}

pub fn leader() -> Actor {
    Actor {
        id: ActorId::from_u128(101),
        kind: ActorKind::Leader,
    }
}

#[allow(dead_code)]
pub fn worker() -> Actor {
    Actor {
        id: ActorId::from_u128(102),
        kind: ActorKind::Worker,
    }
}

#[allow(dead_code)]
pub fn verifier() -> Actor {
    Actor {
        id: ActorId::from_u128(103),
        kind: ActorKind::Verifier,
    }
}

pub fn task_a() -> TaskId {
    TaskId::from_u128(1)
}

pub fn task_b() -> TaskId {
    TaskId::from_u128(2)
}

pub fn task_c() -> TaskId {
    TaskId::from_u128(3)
}

fn task(
    id: TaskId,
    objective: &str,
    depends_on: Vec<TaskId>,
    data_depends_on: Vec<TaskId>,
    criteria: &str,
) -> PlanTaskV1 {
    PlanTaskV1 {
        id,
        kind: TaskKind::Implementation,
        objective: objective.to_string(),
        inputs: vec!["approved goal".to_string()],
        scope: vec!["crates/central-domain".to_string()],
        out_of_scope: vec!["UI".to_string()],
        depends_on,
        data_depends_on,
        outputs: vec!["code change".to_string()],
        verify: Some(VerifySpec {
            method: "cargo test".to_string(),
            criteria: vec![criteria.to_string()],
        }),
        evidence: vec!["test output".to_string()],
        profile_id: None,
        permissions: vec!["workspace-write".to_string()],
        max_repair_rounds: 2,
        requires_approval: Vec::new(),
        handover: "report changed files and test results".to_string(),
    }
}

pub fn valid_plan() -> PlanDocumentV1 {
    PlanDocumentV1 {
        version: 1,
        goal: "Build the durable runtime".to_string(),
        tasks: vec![
            task(
                task_a(),
                "Implement part A",
                Vec::new(),
                Vec::new(),
                "A tests pass",
            ),
            task(
                task_b(),
                "Implement part B",
                Vec::new(),
                Vec::new(),
                "B tests pass",
            ),
            task(
                task_c(),
                "Integrate A output",
                vec![task_a()],
                vec![task_a()],
                "C tests pass",
            ),
        ],
    }
}

pub fn created_run() -> RunState {
    let state = RunState::new(RunId::from_u128(10));
    decide(
        &state,
        &user(),
        &Command::CreateRun {
            goal: "Build the durable runtime".to_string(),
            repository: "D:/Code/enolalab/central".to_string(),
        },
        now(),
    )
    .expect("create run")
    .next
}

pub fn proposed_run() -> (RunState, central_domain::PlanHash) {
    let state = created_run();
    let decision = decide(
        &state,
        &leader(),
        &Command::ProposePlan { plan: valid_plan() },
        now(),
    )
    .expect("propose plan");
    let hash = decision
        .next
        .pending_plan
        .clone()
        .expect("pending plan hash");
    (decision.next, hash)
}

pub fn approved_run() -> (RunState, central_domain::PlanHash) {
    let (state, hash) = proposed_run();
    let decision = decide(
        &state,
        &user(),
        &Command::ApprovePlan {
            plan_hash: hash.clone(),
        },
        later(),
    )
    .expect("approve plan");
    (decision.next, hash)
}
