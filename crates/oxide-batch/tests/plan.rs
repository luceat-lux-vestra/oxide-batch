//! Flow-graph compilation, bounds, and exit-pattern semantics.

#![allow(clippy::expect_used, clippy::panic)]

use std::error::Error;

use oxide_batch::{
    ComponentRevision, DeciderRevision, DecisionInputVersion, DecisionNode, DefinitionError,
    DefinitionRevision, ExitCode, ExitPattern, FlowGraph, FlowNode, FlowSelectionError, FlowTarget,
    FlowTransition, JobName, MAX_OUTGOING_TRANSITIONS, MAX_PATTERN_BYTES, NodeId, PlanError,
    StartControls, StartLimit, StepComponents, StepName, StepNode, TerminalKind,
};

fn tasklet_node(id: &str) -> Result<FlowNode, Box<dyn Error>> {
    Ok(FlowNode::step(StepNode::new(
        NodeId::new(id)?,
        StepName::new(id)?,
        StepComponents::Tasklet(ComponentRevision::new(format!("{id}-v1"))?),
    )))
}

fn compile(graph: FlowGraph) -> Result<oxide_batch::CompiledExecutionPlan, Box<dyn Error>> {
    Ok(graph.compile(
        &JobName::new("daily_import")?,
        DefinitionRevision::new("plan-v1")?,
    )?)
}

fn two_step_graph() -> Result<FlowGraph, Box<dyn Error>> {
    let load = NodeId::new("load")?;
    let report = NodeId::new("report")?;
    Ok(FlowGraph::new(load.clone())
        .with_node(tasklet_node("load")?)
        .with_node(tasklet_node("report")?)
        .with_sequence(load, FlowTarget::Node(report.clone()))?
        .with_sequence(report, FlowTarget::Terminal(TerminalKind::Complete))?)
}

#[test]
fn sequential_edges_continue_on_success_and_terminate_on_failure() -> Result<(), Box<dyn Error>> {
    let plan = compile(two_step_graph()?)?;
    let load = NodeId::new("load")?;

    assert_eq!(plan.node_count(), 2);
    assert_eq!(plan.transition_count(), 4);
    assert_eq!(
        plan.select_target(&load, &ExitCode::new("COMPLETED")?)?,
        &FlowTarget::Node(NodeId::new("report")?)
    );
    assert_eq!(
        plan.select_target(&load, &ExitCode::new("PARTIAL")?)?,
        &FlowTarget::Node(NodeId::new("report")?)
    );
    assert_eq!(
        plan.select_target(&load, &ExitCode::new("FAILED")?)?,
        &FlowTarget::Terminal(TerminalKind::Fail)
    );
    Ok(())
}

#[test]
fn exit_status_selects_the_most_specific_transition() -> Result<(), Box<dyn Error>> {
    let load = NodeId::new("load")?;
    let plan = compile(
        FlowGraph::new(load.clone())
            .with_node(tasklet_node("load")?)
            .with_transition(FlowTransition::new(
                load.clone(),
                ExitPattern::new("*")?,
                FlowTarget::Terminal(TerminalKind::Fail),
            ))
            .with_transition(FlowTransition::new(
                load.clone(),
                ExitPattern::new("COMPLETED*")?,
                FlowTarget::Terminal(TerminalKind::Stop),
            ))
            .with_transition(FlowTransition::new(
                load.clone(),
                ExitPattern::new("COMPLETED")?,
                FlowTarget::Terminal(TerminalKind::Complete),
            )),
    )?;

    let ordered: Vec<&str> = plan
        .transitions(&load)
        .iter()
        .map(|transition| transition.pattern().as_str())
        .collect();
    assert_eq!(ordered, vec!["COMPLETED", "COMPLETED*", "*"]);
    assert_eq!(
        plan.select_target(&load, &ExitCode::new("COMPLETED")?)?,
        &FlowTarget::Terminal(TerminalKind::Complete)
    );
    assert_eq!(
        plan.select_target(&load, &ExitCode::new("COMPLETED_WITH_WARNINGS")?)?,
        &FlowTarget::Terminal(TerminalKind::Stop)
    );
    assert_eq!(
        plan.select_target(&load, &ExitCode::new("ANYTHING")?)?,
        &FlowTarget::Terminal(TerminalKind::Fail)
    );
    Ok(())
}

#[test]
fn equally_specific_overlapping_patterns_are_rejected() -> Result<(), Box<dyn Error>> {
    let load = NodeId::new("load")?;
    let error = FlowGraph::new(load.clone())
        .with_node(tasklet_node("load")?)
        .with_transition(FlowTransition::new(
            load.clone(),
            ExitPattern::new("A*")?,
            FlowTarget::Terminal(TerminalKind::Complete),
        ))
        .with_transition(FlowTransition::new(
            load,
            ExitPattern::new("*A")?,
            FlowTarget::Terminal(TerminalKind::Fail),
        ))
        .compile(
            &JobName::new("daily_import")?,
            DefinitionRevision::new("plan-v1")?,
        )
        .expect_err("equally specific overlapping patterns must be rejected");

    assert!(matches!(error, PlanError::AmbiguousTransition { .. }));
    Ok(())
}

#[test]
fn equally_specific_disjoint_patterns_are_accepted() -> Result<(), Box<dyn Error>> {
    let load = NodeId::new("load")?;
    let plan = compile(
        FlowGraph::new(load.clone())
            .with_node(tasklet_node("load")?)
            .with_transition(FlowTransition::new(
                load.clone(),
                ExitPattern::new("AB")?,
                FlowTarget::Terminal(TerminalKind::Complete),
            ))
            .with_transition(FlowTransition::new(
                load.clone(),
                ExitPattern::new("AC")?,
                FlowTarget::Terminal(TerminalKind::Fail),
            ))
            .with_transition(FlowTransition::new(
                load.clone(),
                ExitPattern::new("*")?,
                FlowTarget::Terminal(TerminalKind::Stop),
            )),
    )?;

    assert_eq!(
        plan.select_target(&load, &ExitCode::new("AB")?)?,
        &FlowTarget::Terminal(TerminalKind::Complete)
    );
    assert_eq!(
        plan.select_target(&load, &ExitCode::new("AC")?)?,
        &FlowTarget::Terminal(TerminalKind::Fail)
    );
    Ok(())
}

#[test]
fn an_unmapped_exit_outcome_selects_no_default() -> Result<(), Box<dyn Error>> {
    let load = NodeId::new("load")?;
    let plan = compile(
        FlowGraph::new(load.clone())
            .with_node(tasklet_node("load")?)
            .with_transition(FlowTransition::new(
                load.clone(),
                ExitPattern::new("COMPLETED")?,
                FlowTarget::Terminal(TerminalKind::Complete),
            )),
    )?;

    assert_eq!(
        plan.select_target(&load, &ExitCode::new("FAILED")?),
        Err(FlowSelectionError::UnmappedExitOutcome {
            node: load,
            code: ExitCode::new("FAILED")?,
        })
    );
    assert!(matches!(
        plan.select_target(&NodeId::new("absent")?, &ExitCode::new("COMPLETED")?),
        Err(FlowSelectionError::UnknownNode { .. })
    ));
    Ok(())
}

#[test]
fn structural_errors_are_rejected_before_execution() -> Result<(), Box<dyn Error>> {
    let load = NodeId::new("load")?;
    let report = NodeId::new("report")?;

    let duplicate = FlowGraph::new(load.clone())
        .with_node(tasklet_node("load")?)
        .with_node(tasklet_node("load")?)
        .with_sequence(load.clone(), FlowTarget::Terminal(TerminalKind::Complete))?;
    assert!(matches!(
        compile_error(duplicate)?,
        PlanError::DuplicateNodeId { .. }
    ));

    let undefined = FlowGraph::new(load.clone())
        .with_node(tasklet_node("load")?)
        .with_sequence(load.clone(), FlowTarget::Node(report.clone()))?;
    assert!(matches!(
        compile_error(undefined)?,
        PlanError::UndefinedNode { .. }
    ));

    let no_transition = FlowGraph::new(load.clone()).with_node(tasklet_node("load")?);
    assert!(matches!(
        compile_error(no_transition)?,
        PlanError::MissingTransition { .. }
    ));

    let unreachable = FlowGraph::new(load.clone())
        .with_node(tasklet_node("load")?)
        .with_node(tasklet_node("report")?)
        .with_sequence(load.clone(), FlowTarget::Terminal(TerminalKind::Complete))?
        .with_sequence(report.clone(), FlowTarget::Terminal(TerminalKind::Complete))?;
    assert!(matches!(
        compile_error(unreachable)?,
        PlanError::UnreachableNode { .. }
    ));

    let cyclic = FlowGraph::new(load.clone())
        .with_node(tasklet_node("load")?)
        .with_node(tasklet_node("report")?)
        .with_sequence(load, FlowTarget::Node(report.clone()))?
        .with_sequence(report, FlowTarget::Node(NodeId::new("load")?))?;
    assert!(matches!(
        compile_error(cyclic)?,
        PlanError::CyclicGraph { .. }
    ));
    Ok(())
}

fn compile_error(graph: FlowGraph) -> Result<PlanError, Box<dyn Error>> {
    Ok(graph
        .compile(
            &JobName::new("daily_import")?,
            DefinitionRevision::new("plan-v1")?,
        )
        .expect_err("the graph must be rejected"))
}

#[test]
fn one_node_cannot_exceed_the_outgoing_transition_bound() -> Result<(), Box<dyn Error>> {
    let load = NodeId::new("load")?;
    let mut graph = FlowGraph::new(load.clone()).with_node(tasklet_node("load")?);
    for index in 0..=MAX_OUTGOING_TRANSITIONS {
        graph = graph.with_transition(FlowTransition::new(
            load.clone(),
            ExitPattern::new(format!("CODE{index:04}"))?,
            FlowTarget::Terminal(TerminalKind::Complete),
        ));
    }

    assert!(matches!(
        compile_error(graph)?,
        PlanError::TooManyOutgoingTransitions {
            max: MAX_OUTGOING_TRANSITIONS,
            ..
        }
    ));
    Ok(())
}

#[test]
fn exit_patterns_are_bounded_and_printable() {
    let too_long = "A".repeat(MAX_PATTERN_BYTES + 1);
    for rejected in ["", " FAILED", "FAILED ", "FAIL\u{0}ED", too_long.as_str()] {
        assert_eq!(
            ExitPattern::new(rejected),
            Err(PlanError::InvalidPattern {
                max_bytes: MAX_PATTERN_BYTES
            }),
            "pattern {rejected:?} must be rejected"
        );
    }
    assert!(ExitPattern::new("A".repeat(MAX_PATTERN_BYTES)).is_ok());
}

#[test]
fn a_single_character_wildcard_counts_characters_not_bytes() -> Result<(), Box<dyn Error>> {
    let pattern = ExitPattern::new("?")?;
    assert!(pattern.matches(&ExitCode::new("é")?));
    assert!(!pattern.matches(&ExitCode::new("ée")?));
    assert_eq!(pattern.specificity().bytes(), 1);
    Ok(())
}

#[test]
fn specificity_orders_literals_then_wildcards_then_length() -> Result<(), Box<dyn Error>> {
    let exact = ExitPattern::new("FAILED")?;
    let prefix = ExitPattern::new("FAILED*")?;
    let wildcard = ExitPattern::new("*")?;
    let single = ExitPattern::new("??????")?;

    assert!(exact.specificity() > prefix.specificity());
    assert!(prefix.specificity() > wildcard.specificity());
    assert!(wildcard.specificity() > single.specificity());
    assert_eq!(exact.specificity().literals(), 6);
    assert_eq!(prefix.specificity().wildcards(), 1);
    Ok(())
}

const ALPHABET: [char; 2] = ['A', 'B'];
const PATTERN_ALPHABET: [char; 4] = ['A', 'B', '*', '?'];

fn enumerate(alphabet: &[char], max_length: usize) -> Vec<String> {
    let mut values = vec![String::new()];
    let mut frontier = vec![String::new()];
    for _ in 0..max_length {
        let mut next = Vec::new();
        for prefix in &frontier {
            for character in alphabet {
                let mut candidate = prefix.clone();
                candidate.push(*character);
                next.push(candidate);
            }
        }
        values.extend(next.iter().cloned());
        frontier = next;
    }
    values
}

#[test]
fn pattern_matching_agrees_with_an_exhaustive_reference() -> Result<(), Box<dyn Error>> {
    let values = enumerate(&ALPHABET, 4);
    for pattern in enumerate(&PATTERN_ALPHABET, 3) {
        if pattern.is_empty() {
            continue;
        }
        let compiled = ExitPattern::new(pattern.clone())?;
        for value in &values {
            if value.is_empty() {
                continue;
            }
            let expected = reference_matches(
                &pattern.chars().collect::<Vec<_>>(),
                &value.chars().collect::<Vec<_>>(),
            );
            assert_eq!(
                compiled.matches(&ExitCode::new(value.clone())?),
                expected,
                "pattern {pattern:?} against {value:?}"
            );
        }
    }
    Ok(())
}

#[test]
fn pattern_intersection_agrees_with_an_exhaustive_reference() -> Result<(), Box<dyn Error>> {
    // Two patterns of at most three characters share a value only if they
    // share one of at most six characters, so this witness set is complete.
    let witnesses: Vec<Vec<char>> = enumerate(&ALPHABET, 6)
        .into_iter()
        .map(|value| value.chars().collect())
        .collect();
    let patterns: Vec<ExitPattern> = enumerate(&PATTERN_ALPHABET, 3)
        .into_iter()
        .filter(|pattern| !pattern.is_empty())
        .map(ExitPattern::new)
        .collect::<Result<_, _>>()?;

    for (index, left) in patterns.iter().enumerate() {
        let left_chars: Vec<char> = left.as_str().chars().collect();
        for right in &patterns[index..] {
            let right_chars: Vec<char> = right.as_str().chars().collect();
            let expected = witnesses.iter().any(|witness| {
                reference_matches(&left_chars, witness) && reference_matches(&right_chars, witness)
            });
            assert_eq!(
                left.intersects(right),
                expected,
                "patterns {left} and {right}"
            );
        }
    }
    Ok(())
}

fn reference_matches(pattern: &[char], value: &[char]) -> bool {
    match pattern.split_first() {
        None => value.is_empty(),
        Some(('*', rest)) => (0..=value.len()).any(|skip| reference_matches(rest, &value[skip..])),
        Some(('?', rest)) => !value.is_empty() && reference_matches(rest, &value[1..]),
        Some((literal, rest)) => {
            value.first() == Some(literal) && reference_matches(rest, &value[1..])
        }
    }
}

#[test]
fn declaration_order_does_not_change_the_fingerprint() -> Result<(), Box<dyn Error>> {
    let load = NodeId::new("load")?;
    let report = NodeId::new("report")?;
    let forward = compile(
        FlowGraph::new(load.clone())
            .with_node(tasklet_node("load")?)
            .with_node(tasklet_node("report")?)
            .with_sequence(load.clone(), FlowTarget::Node(report.clone()))?
            .with_sequence(report.clone(), FlowTarget::Terminal(TerminalKind::Complete))?,
    )?;
    let reversed = compile(
        FlowGraph::new(load.clone())
            .with_node(tasklet_node("report")?)
            .with_node(tasklet_node("load")?)
            .with_sequence(report, FlowTarget::Terminal(TerminalKind::Complete))?
            .with_sequence(load, FlowTarget::Node(NodeId::new("report")?))?,
    )?;

    assert_eq!(forward.fingerprint(), reversed.fingerprint());
    assert_eq!(
        forward.definition_identity().canonical_manifest(),
        reversed.definition_identity().canonical_manifest()
    );
    Ok(())
}

#[test]
fn restart_relevant_values_change_the_fingerprint() -> Result<(), Box<dyn Error>> {
    let baseline = compile(two_step_graph()?)?;
    let load = NodeId::new("load")?;
    let report = NodeId::new("report")?;
    let limited = compile(
        FlowGraph::new(load.clone())
            .with_node(FlowNode::step(
                StepNode::new(
                    load.clone(),
                    StepName::new("load")?,
                    StepComponents::Tasklet(ComponentRevision::new("load-v1")?),
                )
                .with_start_controls(StartControls::new(StartLimit::new(3)?, true)),
            ))
            .with_node(tasklet_node("report")?)
            .with_sequence(load, FlowTarget::Node(report.clone()))?
            .with_sequence(report, FlowTarget::Terminal(TerminalKind::Complete))?,
    )?;

    assert_ne!(baseline.fingerprint(), limited.fingerprint());
    Ok(())
}

#[test]
fn a_decision_node_is_compiled_and_fingerprinted() -> Result<(), Box<dyn Error>> {
    let choose = NodeId::new("choose")?;
    let load = NodeId::new("load")?;
    let plan = compile(
        FlowGraph::new(choose.clone())
            .with_node(FlowNode::decision(DecisionNode::new(
                choose.clone(),
                DeciderRevision::new("decider-v1")?,
                DecisionInputVersion::new(1)?,
            )))
            .with_node(tasklet_node("load")?)
            .with_transition(FlowTransition::new(
                choose.clone(),
                ExitPattern::new("RUN")?,
                FlowTarget::Node(load.clone()),
            ))
            .with_transition(FlowTransition::new(
                choose.clone(),
                ExitPattern::new("*")?,
                FlowTarget::Terminal(TerminalKind::Complete),
            ))
            .with_sequence(load, FlowTarget::Terminal(TerminalKind::Complete))?,
    )?;

    assert_eq!(plan.node_count(), 2);
    assert_eq!(
        plan.select_target(&choose, &ExitCode::new("RUN")?)?,
        &FlowTarget::Node(NodeId::new("load")?)
    );
    Ok(())
}

#[test]
fn zero_valued_controls_are_rejected() {
    assert_eq!(StartLimit::new(0), Err(DefinitionError::ZeroStartLimit));
    assert_eq!(
        DecisionInputVersion::new(0),
        Err(PlanError::ZeroDecisionInputVersion)
    );
    assert_eq!(StartLimit::default(), StartLimit::UNRESTRICTED);
    assert_eq!(StartLimit::UNRESTRICTED.get(), u32::MAX);
    assert!(!StartControls::default().allow_start_if_complete());
}
