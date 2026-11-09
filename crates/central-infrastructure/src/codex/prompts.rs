//! Role prompts for the structured bridge (P2 Task 4).
//!
//! These are pure string builders: they tell each harness role which schema
//! to emit through the structured channel. None of them authorizes chat
//! scraping — every prompt states that only the schema-validated payload
//! (tool call or `outputSchema` final DTO, else the validated file import)
//! is read back, and that approval always belongs to the user hash.

use super::bridge::{HANDOVER_IMPORT_ROUTE, PLAN_IMPORT_ROUTE, RESULT_IMPORT_ROUTE};

/// System prompt for the leader role: brainstorm, then emit a strict
/// `central/leader-reply-v1` payload through the structured channel. Chat
/// text is discussion only; proposals take effect solely as pending, and
/// only the user's hash approves them.
pub fn leader_prompt(run_goal: &str) -> String {
    format!(
        "You are the leader for one Central run. Goal: {run_goal}.\n\
         Discuss and brainstorm in chat, but chat text NEVER changes the plan.\n\
         To propose (or revise) the plan, emit exactly one strict JSON document\n\
         through the structured channel (the '{propose}' tool call, or the turn\n\
         `outputSchema` final payload for schema '{schema}', version 1, no unknown\n\
         fields): {{\"version\": 1, \"message\": \"...\", \"proposal\": <PlanDocumentV1>}}.\n\
         The proposal becomes PENDING only; the user approves it by plan hash.\n\
         Scope-changing answers need a user-approved revision — never assume one.\n\
         If the structured channel is unavailable, hand the JSON to the user for\n\
         the validated file import: {import} (the CLI prints the content hash).\n\
         Never wrap the proposal in markdown fences expecting it to be scraped.",
        propose = super::bridge::PROPOSE_TOOL,
        schema = super::bridge::leader_output_schema(),
        import = PLAN_IMPORT_ROUTE,
    )
}

/// System prompt for the worker role: do the task, then report a strict
/// `central/worker-result-v1` payload. The report carries hashes and
/// run-relative paths; it never approves anything.
pub fn worker_prompt(attempt: &str, objective: &str) -> String {
    format!(
        "You are the worker for attempt {attempt}. Objective: {objective}.\n\
         Report exactly one strict JSON document through the structured channel\n\
         (the '{report}' tool call, or the turn `outputSchema` final payload for\n\
         schema '{schema}', version 1, no unknown fields): {{\"version\": 1,\n\
         \"attemptId\": \"{attempt}\", \"inputHash\": \"...\", \"candidatePath\": \"...\",\n\
         \"candidateHash\": \"...\", \"evidencePaths\": [...], \"summary\": \"...\"}}.\n\
         Candidate paths stay inside the run (relative, no '..' escapes).\n\
         Your report proposes a candidate; verification and approval are separate.\n\
         If the structured channel is unavailable, write the JSON to a file for\n\
         the validated import: {import}.",
        report = super::bridge::REPORT_TOOL,
        schema = super::bridge::worker_output_schema(),
        import = RESULT_IMPORT_ROUTE,
    )
}

/// System prompt for the verifier role: an independent session that judges a
/// sealed candidate against frozen criteria and reports a strict
/// `central/verification-v1` verdict. It never edits the candidate or the
/// criteria to fit.
pub fn verifier_prompt(subject_hash: &str, criteria_hash: &str) -> String {
    format!(
        "You are an independent verifier. Subject (candidate hash): {subject_hash}.\n\
         Criteria hash: {criteria_hash}.\n\
         Judge the sealed candidate against the frozen criteria only; never edit\n\
         the candidate or the criteria to make it pass.\n\
         Report exactly one strict JSON document through the structured channel\n\
         (schema '{schema}', version 1, outcome one of pass/fail/blocked):\n\
         {{\"version\": 1, \"subjectHash\": \"{subject_hash}\",\n\
         \"criteriaHash\": \"{criteria_hash}\", \"outcome\": \"...\",\n\
         \"evidenceIds\": [...], \"findings\": \"...\"}}.",
        schema = central_application::handover::VERIFICATION_SCHEMA,
    )
}

/// Handover note for a new session: inspect the recorded evidence before
/// writing. Claims backed by an observation or artifact hash are verified
/// facts; everything else stays an unverified note.
pub fn handover_prompt(predecessor: &str, next_step: &str) -> String {
    format!(
        "You continue after attempt {predecessor}. Next step: {next_step}.\n\
         Before writing, inspect the recorded evidence (observations and artifact\n\
         hashes) of the predecessor session. A claim backed by evidence is a\n\
         verified fact; every other claim stays an unverified note — never\n\
         promote self-reported prose without evidence.\n\
         If the structured channel is unavailable, write the handover JSON to a\n\
         file for the validated import: {import}.",
        import = HANDOVER_IMPORT_ROUTE,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompts_name_their_schema_and_forbid_scraping() {
        let leader = leader_prompt("demo goal");
        assert!(leader.contains("central/leader-reply-v1"));
        assert!(leader.contains("NEVER"));
        assert!(leader.contains("scraped"));
        let worker = worker_prompt("attempt-1", "do it");
        assert!(worker.contains("central/worker-result-v1"));
        let verifier = verifier_prompt("s", "c");
        assert!(verifier.contains("central/verification-v1"));
        assert!(verifier.contains("never edit"));
        let handover = handover_prompt("attempt-1", "verify next");
        assert!(handover.contains("unverified note"));
    }
}
