#!/bin/sh
# Model-check ShellSession, the session the interactive app has open and what
# `/status` and the exit line say about it (D90), and require each config's
# expected verdict.
#
# holds.cfg must pass. Each witness-*.cfg must be violated, which shows the
# state it names is reachable, so the properties are not vacuous there. Each
# mutant-*.cfg plants one bug that its property must catch. simulate.cfg runs
# random behaviors with more ids and turns; SIM_RUNS sets how many (default
# 20000).
#
# Run the real binary along the model and have TLC check every run:
#   bun test ../drivers/session/
#
# Needs Java 11+ and tla2tools.jar, as ../README.md describes.
set -eu

cd "$(dirname "$0")"
# shellcheck source=../lib/tlc.sh
. ../lib/tlc.sh

sim="-simulate num=${SIM_RUNS:-20000} -depth 40 -seed 1"
tlc_verdicts "$@" <<EOF
ShellSession holds -
ShellSession witness-saved-empty-status NoSavedEmptyStatus
ShellSession witness-unsaved-status NoUnsavedStatus
ShellSession witness-storeless-status NoStorelessStatus
ShellSession witness-empty-exit-line NoEmptyExitLine
ShellSession witness-resume-after-crash NoResumeAfterCrash
ShellSession mutant-saved-by-turns StatusUnsavedIsAbsent
ShellSession mutant-saved-on-open StatusSavedIsOnDisk
ShellSession mutant-exit-by-turns ExitAgreesWithStatus
ShellSession mutant-status-launch-id StatusNamesOpenSession
ShellSession mutant-keep-pristine DiscardedNeverSaved
ShellSession mutant-reuse-id NewIdsAreFresh
ShellSession mutant-fork-held ForkOnDisk
ShellSession mutant-drop-on-switch SavedStaysSaved
ShellSession simulate simulate $sim
EOF
