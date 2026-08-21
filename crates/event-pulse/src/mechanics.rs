//! Pure, deterministic bootstrap phase classification.

use thiserror::Error;

use crate::features::{Direction, SCALE};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Normal,
    Buildup,
    Ignition,
    Cascade,
    Exhaustion,
    Aftermath,
    Invalid,
}

impl Phase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Buildup => "BUILDUP",
            Self::Ignition => "IGNITION",
            Self::Cascade => "CASCADE",
            Self::Exhaustion => "EXHAUSTION",
            Self::Aftermath => "AFTERMATH",
            Self::Invalid => "INVALID",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FamilyFlags {
    pub price: bool,
    pub flow: bool,
    pub book: bool,
    pub derivatives: bool,
    pub breadth: bool,
}

impl FamilyFlags {
    pub fn count(self) -> usize {
        [
            self.price,
            self.flow,
            self.book,
            self.derivatives,
            self.breadth,
        ]
        .into_iter()
        .filter(|active| *active)
        .count()
    }

    pub fn intensity(self) -> i128 {
        i128::from(self.price) * 25_000_000
            + i128::from(self.flow) * 25_000_000
            + i128::from(self.book) * 15_000_000
            + i128::from(self.derivatives) * 20_000_000
            + i128::from(self.breadth) * 15_000_000
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MechanicsEvidence {
    pub available_at_ns: i64,
    pub direction: Direction,
    pub families: FamilyFlags,
    pub intensity: i128,
    pub confidence: i128,
    pub reversal_risk: i128,
    pub valid: bool,
    pub fully_warmed: bool,
    pub spread_bps: i128,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PhaseError {
    #[error("phase time decreased")]
    TimeRegression,
    #[error("mechanics score is outside the canonical ratio domain")]
    ScoreDomain,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Candidate {
    target: Phase,
    start_ns: i64,
    dwell_ns: i64,
}

#[derive(Debug, Clone)]
pub struct PhaseMachine {
    phase: Phase,
    evidence: Option<MechanicsEvidence>,
    candidate: Option<Candidate>,
    now_ns: Option<i64>,
}

impl Default for PhaseMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl PhaseMachine {
    pub fn new() -> Self {
        Self {
            phase: Phase::Normal,
            evidence: None,
            candidate: None,
            now_ns: None,
        }
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    pub fn evidence(&self) -> Option<&MechanicsEvidence> {
        self.evidence.as_ref()
    }

    pub fn observe(&mut self, evidence: &MechanicsEvidence) -> Result<(), PhaseError> {
        if self
            .now_ns
            .is_some_and(|now| evidence.available_at_ns < now)
        {
            return Err(PhaseError::TimeRegression);
        }
        if !(0..=SCALE).contains(&evidence.intensity)
            || !(0..=SCALE).contains(&evidence.confidence)
            || !(0..=SCALE).contains(&evidence.reversal_risk)
            || evidence.intensity != evidence.families.intensity()
        {
            return Err(PhaseError::ScoreDomain);
        }
        self.now_ns = Some(evidence.available_at_ns);
        self.evidence = Some(evidence.clone());
        if !evidence.valid {
            self.phase = Phase::Invalid;
            self.candidate = None;
            return Ok(());
        }
        self.refresh_candidate(evidence.available_at_ns);
        Ok(())
    }

    pub fn advance_to(&mut self, at_ns: i64) -> Result<(), PhaseError> {
        if self.now_ns.is_some_and(|now| at_ns < now) {
            return Err(PhaseError::TimeRegression);
        }
        self.now_ns = Some(at_ns);
        self.refresh_candidate(at_ns);
        Ok(())
    }

    fn refresh_candidate(&mut self, at_ns: i64) {
        let Some(evidence) = self.evidence.as_ref() else {
            return;
        };
        let Some((target, dwell_ns)) = transition(self.phase, evidence) else {
            self.candidate = None;
            return;
        };
        let candidate = match self.candidate {
            Some(candidate) if candidate.target == target => candidate,
            _ => Candidate {
                target,
                start_ns: at_ns,
                dwell_ns,
            },
        };
        if at_ns.saturating_sub(candidate.start_ns) >= candidate.dwell_ns {
            self.phase = candidate.target;
            self.candidate = None;
            // A new phase may immediately begin a different dwell at this same
            // deterministic phase time, but never chains a zero-time transition.
            if candidate.dwell_ns != 0 {
                if let Some((next, dwell_ns)) = transition(self.phase, evidence) {
                    self.candidate = Some(Candidate {
                        target: next,
                        start_ns: at_ns,
                        dwell_ns,
                    });
                }
            }
        } else {
            self.candidate = Some(candidate);
        }
    }
}

fn transition(phase: Phase, e: &MechanicsEvidence) -> Option<(Phase, i64)> {
    if !e.valid {
        return Some((Phase::Invalid, 0));
    }
    let high = e.intensity >= 80_000_000 && e.families.price && e.families.flow;
    match phase {
        Phase::Invalid if e.fully_warmed && e.intensity < 40_000_000 => {
            Some((Phase::Normal, 1_000_000_000))
        }
        Phase::Normal if high => Some((Phase::Ignition, 100_000_000)),
        Phase::Normal
            if e.intensity >= 60_000_000
                && e.confidence >= 80_000_000
                && e.families.count() >= 2 =>
        {
            Some((Phase::Buildup, 250_000_000))
        }
        Phase::Buildup if high => Some((Phase::Ignition, 100_000_000)),
        Phase::Buildup if e.intensity < 40_000_000 => Some((Phase::Normal, 500_000_000)),
        Phase::Ignition
            if e.intensity >= 85_000_000 && (e.families.derivatives || e.families.breadth) =>
        {
            Some((Phase::Cascade, 250_000_000))
        }
        Phase::Ignition if e.reversal_risk >= 65_000_000 => Some((Phase::Exhaustion, 250_000_000)),
        Phase::Cascade if e.reversal_risk >= 65_000_000 || e.intensity <= 40_000_000 => {
            Some((Phase::Exhaustion, 250_000_000))
        }
        Phase::Exhaustion
            if e.intensity >= 85_000_000
                && e.reversal_risk < 35_000_000
                && (e.families.derivatives || e.families.breadth) =>
        {
            Some((Phase::Cascade, 250_000_000))
        }
        Phase::Exhaustion if e.intensity < 60_000_000 && e.reversal_risk >= 50_000_000 => {
            Some((Phase::Aftermath, 1_000_000_000))
        }
        Phase::Aftermath if e.intensity >= 60_000_000 && e.reversal_risk < 35_000_000 => {
            Some((Phase::Buildup, 500_000_000))
        }
        Phase::Aftermath
            if e.intensity < 40_000_000
                && e.reversal_risk < 20_000_000
                && e.spread_bps < 8 * SCALE =>
        {
            Some((Phase::Normal, 5_000_000_000))
        }
        _ => None,
    }
}
