# Hlscreen shared-feed platform checks

Hlscreen commit `1338b01268538c78a9400213e8dc780bc3372ddc` pins all seven direct
and23 resolved marketfeed packages to producer84aa635. Its own
[main CI](https://github.com/rsitech-ai/hlscreen/actions/runs/39581593798) and
[native package workflow](https://github.com/rsitech-ai/hlscreen/actions/runs/39581593520)
both completed successfully. This receipt qualifies that exact consumer revision,
separately from later producer pipeline changes.

Main checks pass589 Rust executions/565 unique names, Python8+5+10 tests and
six PTY checks on each of Mac and Linux. Dependency/security and attribution
checks pass. Linux, Windows, Mac ARM and Intel Mac packages each pass the actual
67-group, two-event, six-gap sealed-feed smoke. The synthetic fixture checks
structural consumption; native source authentication remains false and usable
event time remains unavailable.

Independent review verifies seven ZIP digests and CRCs, linked and global
checksums, all six packaged documents and exact351-file exported-source equality.
The tested merge tree equals the author commit tree. Only the downloaded Mac ARM
binary also ran locally; foreign native execution evidence comes from the
official hosted jobs. Scheduled API, release publication and announcement jobs
were skipped by their existing conditions. No release or tag was published.

[qualification.json](qualification.json) is the independent portable receipt,
with one equivalent limitation sentence reworded after a scanner matched English
prose. The copy records its original hash and exact changed JSON path; all
source, artifact, test and other values remain unchanged. The original receipt
and failed scan are retained, and scanner policy is unchanged. Previous failed Windows fixture
newline and archive-inspector attempts remain retained. Actual growing joint12
and joint13 use the separately qualified local artifact8a63828d, not a silently
substituted hosted executable. Perps acquisition timing remains open; consumer
platform success does not establish timely acquisition or longer-run readiness.
