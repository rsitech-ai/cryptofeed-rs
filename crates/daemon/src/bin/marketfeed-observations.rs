//! Read-only MFOP1 JSONL bridge. Structural validation does not authenticate the
//! original native source or establish historical feature availability.
use marketfeed_engine::causal::projection::{ProjectionGroup, ProjectionLimits, ProjectionReader};
use marketfeed_recording::shared_feed::FeedCursor;
use serde::{Deserialize, Serialize};
use std::io::{self, BufRead, Write};
use std::path::PathBuf;

const INPUT_BYTES: usize = 16 << 10;
const OUTPUT_BYTES: usize = 16 << 20;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct CursorPair {
    target: Cursor,
    source: Option<Cursor>,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    feed_id: String,
    sequence: u64,
    offset: u64,
}
impl From<FeedCursor> for Cursor {
    fn from(value: FeedCursor) -> Self {
        Self {
            feed_id: value.feed_id,
            sequence: value.sequence,
            offset: value.offset,
        }
    }
}
impl From<Cursor> for FeedCursor {
    fn from(value: Cursor) -> Self {
        Self {
            feed_id: value.feed_id,
            sequence: value.sequence,
            offset: value.offset,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Open {
    directory: PathBuf,
    resume: Option<CursorPair>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ack {
    id: u64,
    checkpoint: CursorPair,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    schema_version: u16,
    #[serde(default, deserialize_with = "present_command")]
    open: Option<Open>,
    #[serde(default, deserialize_with = "present_command")]
    next: Option<Empty>,
    #[serde(default, deserialize_with = "present_command")]
    ack: Option<Ack>,
}
// Missing commands default to None; an explicitly present command must contain
// its typed object. Option's normal deserializer would also accept present null.
fn present_command<'de, D, T>(deserializer: D) -> std::result::Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}
#[derive(Serialize)]
struct Status<'a> {
    checkpoint: &'a CursorPair,
    sealed: bool,
    validation: &'static str,
    native_source_verified: bool,
    availability_time_ns: Option<i64>,
}
#[derive(Serialize)]
struct Offer<'a> {
    id: u64,
    before: &'a CursorPair,
    after: &'a CursorPair,
    sealed: bool,
    group: &'a ProjectionGroup,
    validation: &'static str,
    native_source_verified: bool,
    availability_time_ns: Option<i64>,
}
#[derive(Serialize)]
struct Idle<'a> {
    checkpoint: &'a CursorPair,
    sealed: bool,
    validation: &'static str,
    native_source_verified: bool,
    availability_time_ns: Option<i64>,
}
#[derive(Serialize)]
struct Acknowledged<'a> {
    id: u64,
    checkpoint: &'a CursorPair,
    sealed: bool,
    validation: &'static str,
    native_source_verified: bool,
    availability_time_ns: Option<i64>,
}
#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Body<'a> {
    Opened(Status<'a>),
    Offer(Offer<'a>),
    CaughtUp(Idle<'a>),
    Acknowledged(Acknowledged<'a>),
}
#[derive(Serialize)]
struct Response<'a> {
    schema_version: u16,
    #[serde(flatten)]
    body: Body<'a>,
}
struct Capped(Vec<u8>);
impl Write for Capped {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > (OUTPUT_BYTES - 1).saturating_sub(self.0.len()) {
            return Err(io::Error::other("projection bridge output limit"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn respond(output: &mut impl Write, body: Body<'_>) -> Result<()> {
    // No partially serialized offer reaches stdout, including on a size error.
    let mut encoded = Capped(Vec::new());
    serde_json::to_writer(
        &mut encoded,
        &Response {
            schema_version: 1,
            body,
        },
    )?;
    encoded.0.push(b'\n');
    output.write_all(&encoded.0)?;
    output.flush()?;
    Ok(())
}
fn limits() -> ProjectionLimits {
    ProjectionLimits {
        max_group_bytes: 8 << 20,
        max_items: 8192,
        max_owners: 10,
        max_bootstrap_bytes: 16 << 20,
        max_catalog_entries: 10_000,
        max_pending_inputs: 4096,
    }
}
struct Pending {
    id: u64,
    after: CursorPair,
    sealed: bool,
}
struct Session {
    reader: ProjectionReader,
    acknowledged: CursorPair,
    sealed: bool,
    offered: u64,
    pending: Option<Pending>,
}
impl Session {
    fn open(open: Open) -> Result<Self> {
        if !open.directory.is_absolute() {
            return Err("absolute feed directory required".into());
        }
        let reader = match open.resume {
            Some(pair) => ProjectionReader::resume(
                open.directory,
                pair.target.into(),
                pair.source.map(Into::into),
                limits(),
            )?,
            None => ProjectionReader::open(open.directory, limits())?,
        };
        let p = reader.progress()?;
        Ok(Self {
            acknowledged: CursorPair {
                target: p.target_cursor.into(),
                source: p.source_cursor.map(Into::into),
            },
            sealed: p.sealed,
            reader,
            offered: 0,
            pending: None,
        })
    }
    fn next(&mut self, output: &mut impl Write) -> Result<()> {
        if self.pending.is_some() {
            return Err("acknowledge the pending group before next".into());
        }
        match self.reader.read_next()? {
            None => respond(
                output,
                Body::CaughtUp(Idle {
                    checkpoint: &self.acknowledged,
                    sealed: self.sealed,
                    validation: "mfop1_structure",
                    native_source_verified: false,
                    availability_time_ns: None,
                }),
            ),
            Some(group) => {
                let p = self.reader.progress()?;
                let after = CursorPair {
                    target: p.target_cursor.into(),
                    source: p.source_cursor.map(Into::into),
                };
                self.offered = self.offered.checked_add(1).ok_or("offer id exhausted")?;
                self.pending = Some(Pending {
                    id: self.offered,
                    after,
                    sealed: p.sealed,
                });
                let pending = self.pending.as_ref().expect("new pending offer");
                respond(
                    output,
                    Body::Offer(Offer {
                        id: pending.id,
                        before: &self.acknowledged,
                        after: &pending.after,
                        sealed: pending.sealed,
                        group: &group,
                        validation: "mfop1_structure",
                        native_source_verified: false,
                        availability_time_ns: None,
                    }),
                )
            }
        }
    }
    fn ack(&mut self, ack: Ack, output: &mut impl Write) -> Result<()> {
        let pending = self
            .pending
            .as_ref()
            .ok_or("no pending group to acknowledge")?;
        if ack.id != pending.id || ack.checkpoint != pending.after {
            return Err("acknowledgment does not match the whole offered group".into());
        }
        // A failed stdout write leaves the caller uncertain. It must discard its
        // provisional state and rebuild; neither endpoint invents an ACK receipt.
        respond(
            output,
            Body::Acknowledged(Acknowledged {
                id: pending.id,
                checkpoint: &pending.after,
                sealed: pending.sealed,
                validation: "mfop1_structure",
                native_source_verified: false,
                availability_time_ns: None,
            }),
        )?;
        let pending = self.pending.take().expect("verified pending offer");
        self.acknowledged = pending.after;
        self.sealed = pending.sealed;
        Ok(())
    }
}
fn run(input: &mut impl BufRead, output: &mut impl Write) -> Result<()> {
    let mut session: Option<Session> = None;
    loop {
        let mut line = Vec::new();
        let bytes = std::io::Read::take(&mut *input, (INPUT_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)?;
        if bytes == 0 {
            if session.as_ref().is_some_and(|s| s.pending.is_some()) {
                return Err("EOF with an unacknowledged projection group".into());
            }
            return Ok(());
        }
        if bytes > INPUT_BYTES || line.last() != Some(&b'\n') {
            return Err("bounded newline-terminated bridge request required".into());
        }
        let request: Request = serde_json::from_slice(&line)?;
        if request.schema_version != 1 {
            return Err("unsupported bridge schema".into());
        }
        match (request.open, request.next, request.ack) {
            (Some(open), None, None) if session.is_none() => {
                let opened = Session::open(open)?;
                respond(
                    output,
                    Body::Opened(Status {
                        checkpoint: &opened.acknowledged,
                        sealed: opened.sealed,
                        validation: "mfop1_structure",
                        native_source_verified: false,
                        availability_time_ns: None,
                    }),
                )?;
                session = Some(opened);
            }
            (None, Some(_), None) => session.as_mut().ok_or("open a feed first")?.next(output)?,
            (None, None, Some(ack)) => session
                .as_mut()
                .ok_or("open a feed first")?
                .ack(ack, output)?,
            _ => return Err("exactly one valid bridge command required".into()),
        }
    }
}
fn main() {
    let result = if std::env::args_os().len() != 1 {
        Err("marketfeed-observations accepts JSONL requests on stdin, no arguments".into())
    } else {
        run(&mut io::stdin().lock(), &mut io::stdout().lock())
    };
    if let Err(error) = result {
        eprintln!("projection bridge failed: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor as Input;

    #[test]
    fn malformed_ambiguous_and_oversize_requests_fail_without_output() {
        for raw in [
            br#"{"schema_version":1,"next":{},"ack":{"id":1,"checkpoint":{"target":{"feed_id":"x","sequence":0,"offset":48},"source":null}}}"#.to_vec(),
            br#"{"schema_version":1,"schema_version":1,"next":{}}"#.to_vec(),
            br#"{"schema_version":2,"next":{}}"#.to_vec(),
            br#"{"schema_version":1,"next":{"extra":true}}"#.to_vec(),
            br#"{"schema_version":1,"next":{}}"#.to_vec(),
            vec![b'x'; INPUT_BYTES],
        ] {
            let mut request=raw;request.push(b'\n');let mut output=Vec::new();
            assert!(run(&mut Input::new(request), &mut output).is_err());
            assert!(output.is_empty());
        }
        let mut output = Vec::new();
        assert!(run(&mut Input::new(b"{}"), &mut output).is_err());
        assert!(output.is_empty());
    }

    #[test]
    fn present_null_commands_are_rejected_even_with_one_valid_sibling() {
        for raw in [
            r#"{"schema_version":1,"open":null,"next":{}}"#,
            r#"{"schema_version":1,"ack":null,"next":{}}"#,
            r#"{"schema_version":1,"open":{"directory":"/unused","resume":null},"next":null}"#,
            r#"{"schema_version":1,"open":{"directory":"/unused","resume":null},"ack":null}"#,
        ] {
            assert!(serde_json::from_str::<Request>(raw).is_err(), "{raw}");
        }
        assert!(serde_json::from_str::<Request>(r#"{"schema_version":1,"next":{}}"#).is_ok());
        assert!(
            serde_json::from_str::<Request>(
                r#"{"schema_version":1,"open":{"directory":"/unused","resume":null}}"#
            )
            .is_ok()
        );
    }

    #[test]
    fn output_is_fully_staged_before_emission_and_authority_is_explicit() {
        let pair = CursorPair {
            target: Cursor {
                feed_id: "x".repeat(OUTPUT_BYTES),
                sequence: 0,
                offset: 48,
            },
            source: None,
        };
        let mut output = Vec::new();
        assert!(
            respond(
                &mut output,
                Body::CaughtUp(Idle {
                    checkpoint: &pair,
                    sealed: false,
                    validation: "mfop1_structure",
                    native_source_verified: false,
                    availability_time_ns: None
                })
            )
            .is_err()
        );
        assert!(output.is_empty());
        let pair = CursorPair {
            target: Cursor {
                feed_id: "x".into(),
                sequence: 0,
                offset: 48,
            },
            source: None,
        };
        respond(
            &mut output,
            Body::Acknowledged(Acknowledged {
                id: 1,
                checkpoint: &pair,
                sealed: false,
                validation: "mfop1_structure",
                native_source_verified: false,
                availability_time_ns: None,
            }),
        )
        .unwrap();
        let raw = String::from_utf8(output).unwrap();
        assert!(raw.contains("\"validation\":\"mfop1_structure\""));
        assert!(raw.contains("\"native_source_verified\":false"));
        assert!(raw.contains("\"availability_time_ns\":null"));
        assert_eq!(raw.bytes().filter(|b| *b == b'\n').count(), 1);
    }
}
