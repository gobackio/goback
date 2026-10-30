---------------------------------- MODULE GC ----------------------------------
(* The store's garbage collection protocol: writers deduplicate against what
   the store says it holds, one maintainer condemns unreachable objects with
   tombstones, waits out the sessions that may have deduplicated before the
   tombstones, confirms with a second mark and only then deletes. With
   Resurrect, committing sessions take back what they relied on instead of
   being waited out; with Pipelined, each mark confirms the previous
   generation and condemns for the next, as the store runs it. With
   Unretiring, an operator revives a retired backup whose objects all still
   have copies, while no maintainer runs, and it is a root again. With
   Compaction, a rewrite moves a tombstone out of the archive it was written
   in, keeping its version. With SplitUntomb, a committing session stores its
   commit's un-tombstone with its data and looks up tombstones only after.
   With Delayed, a client decides what the store holds before its session
   begins, as one that uploads several backups in one session long after
   it took them does, and checks those objects again once it has begun.
   With Tries, a session stores the records of commits the store then
   refuses, before or after the session committed; collections take a
   commit record no tombstone names as a root, so never condemn one.
   One session's backup stands for all the backups it commits: each of its
   roots is retired on its own.

   Safe: no committed backup ever references an object without a committed
   copy. Collectable: no horizon passes over a tombstone the store holds, so
   none stands for ever over what it condemns. Refused: a refused commit's
   record is never committed without a newer committed tombstone. *)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Objects,     \* object hashes
    Edges,       \* 10 * parent + child: a tree or file references its children
    Sessions,    \* each session runs once
    Maintainers, \* processes that may run maintenance
    Versions,    \* the write versions a record may get
    Monotone,    \* TRUE: every write gets a newer version than all before it
    MaxCycles,   \* leases the model hands out
    SkipWait,    \* mutation: delete without waiting for older sessions
    SkipConfirm, \* mutation: delete on the first mark alone
    SkipTombs,   \* mutation: Has ignores tombstones
    SkipSnapshot, \* mutation: a rewrite also drops copies written after the snapshot
    NoFence,     \* mutation: maintainers ignore the lease
    Resurrect,   \* sessions resurrect at commit instead of being waited out
    SkipSealCheck, \* mutation: a committing session ignores the seal
    EarlyUntombs, \* mutation: the maintainer reads un-tombstones before sealing
    SkipClosure, \* mutation: an un-tombstone keeps its object but not what it references
    Rewriters,   \* processes that only execute plans other maintainers published
    Handoff,     \* a maintainer publishes its plan for a rewriter instead of deleting
    SkipPlanWait, \* mutation: a mark starts while a published plan is still pending
    Pipelined,   \* one mark confirms the previous generation and condemns for the next
    SealScope,   \* a committing session only re-checks seals written after it began
    SkipUntombRoots, \* mutation: a pipelined mark does not keep what un-tombstones take back
    TombsNewer,  \* a pipelined condemnation checks its tombstones are newer than the snapshot's copies
    SkipPendingRoots, \* mutation: a mark ignores the un-tombstones of sessions that have not committed
    SkipCover,   \* mutation: a session's un-tombstones need not reach all it deduplicated
    Unretiring,  \* an operator revives retired backups
    SkipReviveCheck, \* mutation: a revival does not check that every object it reaches has a copy
    SkipReviveLock,  \* mutation: a revival runs while a maintainer is mid-run
    Compaction,  \* compaction moves tombstones out of the archives they were written in
    SkipCarried, \* mutation: a horizon covers a moved tombstone only if an earlier one did
    SplitUntomb, \* the commit's un-tombstone is stored with its data, before the session looks up tombstones
    LateUntomb,  \* mutation: the commit's un-tombstone is stored after the session read the seals
    Delayed,     \* sessions deduplicate against what the store held before they began
    SkipRecheck, \* mutation: a delayed session keeps what it assumed without asking again
    Tries,       \* commit records the store refuses, disjoint from Objects
    SkipCommittedRefusal, \* mutation: a refusal after its session committed stores no tombstone
    KeepRefusingSession   \* mutation: a session goes on when its refusal's tombstone cannot be stored

None == "none"

VARIABLES
    records,  \* the bucket: copies and tombstones
    clock,    \* the newest version written, for Monotone
    phase,    \* per session: new, active, committed, aborted
    refs,     \* per session: the objects its backup will reference
    roots,    \* <<session, object>> for every object a committed backup references
    gc,       \* per maintainer
    lease,    \* the maintainer holding the lease, or None
    cycles,
    dedup,    \* per session: the objects it deduplicated instead of uploading
    sealed,   \* the tombstones a maintainer sealed before reading un-tombstones
    plans,    \* the plans published to the bucket and not yet removed
    rw,       \* per rewriter
    state,    \* the previous generation's record in the bucket, for a pipelined mark
    seen,     \* per session: the tombstones sealed before it began
    retired,  \* the roots retired, which a revival may take back
    moved,    \* the tombstones compaction moved out of the archive they were written in
    answer    \* per try: none, the session that sent it, or refused

vars == <<records, clock, phase, refs, roots, gc, lease, cycles, dedup, sealed, plans, rw, state, seen, retired, moved, answer>>

\* writer tells copies apart: each session writes an object at most once, and
\* the maintainer writes tombstones
Record(kind, o, v, writer, owner) == [kind |-> kind, obj |-> o, ver |-> v, writer |-> writer, owner |-> owner]

Children(o) == {c \in Objects : 10 * o + c \in Edges}

\* everything a set of objects references, themselves included
RECURSIVE Closure(_)
Closure(S) == IF S = {} THEN {} ELSE S \cup Closure(UNION {Children(o) : o \in S})

Reachable == Closure({p[2] : p \in roots})

Committed == {r \in records : r.owner = None}

\* the records a session's lookups see: everything committed plus its own
\* pending uploads
Visible(s) == {r \in records : r.owner \in {None, s}}

\* an un-tombstone takes a tombstone back when it is newer, or when its
\* session was still live as the tombstone was sealed
Revoked(t, untombs, live) == \E u \in untombs : u.obj = t.obj /\ (u.ver > t.ver \/ u.writer \in live)

\* Has: the newest record of the object decides; a tie goes to the tombstone
Present(s, o) ==
    LET untombs == {u \in Visible(s) : u.kind = "untomb"}
    IN \E c \in Visible(s) :
        /\ c.kind = "copy" /\ c.obj = o
        /\ SkipTombs \/ \A t \in Visible(s) :
              (t.kind = "tomb" /\ t.obj = o) => (t.ver < c.ver \/ Revoked(t, untombs, {}))

Idle == [pc |-> "idle", m1 |-> {}, m2 |-> {}, snap |-> {}, tombs |-> {}, untombs |-> {}, older |-> {}, live |-> {}, drop |-> {}, stood |-> {}]

NoState == [valid |-> FALSE, snap |-> {}, m1 |-> {}, tombs |-> {}, untombs |-> {}, live |-> {}, stood |-> {}]

Ended(s) == phase[s] \in {"committed", "aborted"}

NewVersions == IF Monotone THEN {clock + 1} ELSE Versions

Init ==
    /\ records = {}
    /\ clock = 0
    /\ phase = [s \in Sessions |-> "new"]
    /\ refs = [s \in Sessions |-> {}]
    /\ roots = {}
    /\ gc = [m \in Maintainers |-> Idle]
    /\ lease = None
    /\ cycles = 0
    /\ dedup = [s \in Sessions |-> {}]
    /\ sealed = {}
    /\ plans = {}
    /\ rw = [w \in Rewriters |-> [pc |-> "idle", plan |-> {}]]
    /\ state = NoState
    /\ seen = [s \in Sessions |-> {}]
    /\ retired = {}
    /\ moved = {}
    /\ answer = [c \in Tries |-> "none"]

(* sessions *)

Begin(s) ==
    /\ phase[s] = "new"
    /\ phase' = [phase EXCEPT ![s] = IF Delayed THEN "begun" ELSE "active"]
    /\ seen' = [seen EXCEPT ![s] = sealed]
    /\ UNCHANGED <<answer, moved, records, clock, refs, roots, gc, lease, cycles, dedup, sealed, plans, rw, state, retired>>

\* the session needs o: it deduplicates when the store says o is present and
\* uploads its own copy otherwise
Use(s, o) ==
    /\ phase[s] = "active"
    /\ o \notin refs[s]
    /\ refs' = [refs EXCEPT ![s] = @ \cup {o}]
    /\ IF Present(s, o)
         THEN /\ dedup' = [dedup EXCEPT ![s] = @ \cup {o}]
              /\ UNCHANGED <<answer, moved, records, clock, state, seen>>
         ELSE /\ \E v \in NewVersions :
                   /\ records' = records \cup {Record("copy", o, v, s, s)}
                   /\ clock' = IF v > clock THEN v ELSE clock
              /\ UNCHANGED dedup
    /\ UNCHANGED <<answer, moved, phase, roots, gc, lease, cycles, sealed, plans, rw, state, seen, retired>>

\* before its session begins, a delayed client takes o as stored because the
\* store held it then
Assume(s, o) ==
    /\ Delayed
    /\ phase[s] = "new"
    /\ o \notin refs[s]
    /\ Present(s, o)
    /\ refs' = [refs EXCEPT ![s] = @ \cup {o}]
    /\ dedup' = [dedup EXCEPT ![s] = @ \cup {o}]
    /\ UNCHANGED <<answer, moved, records, clock, phase, roots, gc, lease, cycles, sealed, plans, rw, state, seen, retired>>

\* once begun, it asks again about everything it assumed and uploads what
\* the store no longer says it holds
Recheck(s) ==
    LET gone == IF SkipRecheck THEN {} ELSE {o \in dedup[s] : ~Present(s, o)}
    IN /\ phase[s] = "begun"
       /\ phase' = [phase EXCEPT ![s] = "active"]
       /\ dedup' = [dedup EXCEPT ![s] = @ \ gone]
       /\ \E ver \in [gone -> NewVersions] :
            /\ records' = records \cup {Record("copy", o, ver[o], s, s) : o \in gone}
            /\ clock' = IF gone = {} THEN clock ELSE
                          LET top == CHOOSE v \in {ver[o] : o \in gone} : \A o \in gone : ver[o] <= v
                          IN IF top > clock THEN top ELSE clock
       /\ UNCHANGED <<answer, moved, refs, roots, gc, lease, cycles, sealed, plans, rw, state, seen, retired>>

\* before committing, take back what the session deduplicated: un-tombstone
\* objects whose closure holds all of it, the session's own commit among
\* them, and every deduplicated object a tombstone stands against
Untomb(s) ==
    LET tombed == {o \in dedup[s] : \E t \in Visible(s) : t.kind = "tomb" /\ t.obj = o}
    IN /\ Resurrect
       /\ ~SplitUntomb
       /\ phase[s] = "active"
       /\ phase' = [phase EXCEPT ![s] = "untombed"]
       /\ \E U \in SUBSET refs[s] :
            /\ SkipCover \/ dedup[s] \subseteq Closure(U)
            /\ tombed \subseteq U
            /\ \E ver \in [U -> NewVersions] :
                 /\ records' = records \cup {Record("untomb", o, ver[o], s, s) : o \in U}
                 /\ clock' = IF U = {} THEN clock ELSE
                               LET top == CHOOSE v \in {ver[o] : o \in U} : \A o \in U : ver[o] <= v
                               IN IF top > clock THEN top ELSE clock
       /\ UNCHANGED <<answer, moved, refs, roots, gc, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* with SplitUntomb, Untomb takes two steps: the commit's un-tombstone, whose
\* closure holds all the session deduplicated, is stored with the commit's
\* data; the deduplicated objects a tombstone names, looked up after that,
\* get theirs next
CommitUntomb(s) ==
    /\ Resurrect /\ SplitUntomb
    /\ phase[s] = IF LateUntomb THEN "checked" ELSE "active"
    /\ phase' = [phase EXCEPT ![s] = IF LateUntomb THEN "late" ELSE "flushed"]
    /\ \E U \in SUBSET refs[s] :
         /\ SkipCover \/ dedup[s] \subseteq Closure(U)
         /\ \E ver \in [U -> NewVersions] :
              /\ records' = records \cup {Record("untomb", o, ver[o], s, s) : o \in U}
              /\ clock' = IF U = {} THEN clock ELSE
                            LET top == CHOOSE v \in {ver[o] : o \in U} : \A o \in U : ver[o] <= v
                            IN IF top > clock THEN top ELSE clock
    /\ UNCHANGED <<answer, moved, refs, roots, gc, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

TombedUntomb(s) ==
    LET tombed == {o \in dedup[s] : \E t \in Visible(s) : t.kind = "tomb" /\ t.obj = o}
    IN /\ Resurrect /\ SplitUntomb
       /\ phase[s] = IF LateUntomb THEN "active" ELSE "flushed"
       /\ phase' = [phase EXCEPT ![s] = "untombed"]
       /\ \E ver \in [tombed -> NewVersions] :
            /\ records' = records \cup {Record("untomb", o, ver[o], s, s) : o \in tombed}
            /\ clock' = IF tombed = {} THEN clock ELSE
                          LET top == CHOOSE v \in {ver[o] : o \in tombed} : \A o \in tombed : ver[o] <= v
                          IN IF top > clock THEN top ELSE clock
       /\ UNCHANGED <<answer, moved, refs, roots, gc, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* after its un-tombstones are written, a session uploads again whatever a
\* sealed tombstone condemns among what it deduplicated and all that
\* references
Check(s) ==
    LET checked == IF SealScope THEN sealed \ seen[s] ELSE sealed
        redo == IF SkipSealCheck THEN {} ELSE {o \in Closure(dedup[s]) : \E t \in checked : t.obj = o}
    IN /\ Resurrect
       /\ phase[s] = "untombed"
       \* a re-upload copies the old copy; without one the session can only abort
       /\ \A o \in redo : \E c \in Committed : c.kind = "copy" /\ c.obj = o
       /\ phase' = [phase EXCEPT ![s] = "checked"]
       /\ \E ver \in [redo -> NewVersions] :
            /\ records' = records \cup {Record("copy", o, ver[o], s, s) : o \in redo}
            /\ clock' = IF redo = {} THEN clock ELSE
                          LET top == CHOOSE v \in {ver[o] : o \in redo} : \A o \in redo : ver[o] <= v
                          IN IF top > clock THEN top ELSE clock
       /\ UNCHANGED <<answer, moved, refs, roots, gc, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* an agent uploads an object only after handling what it references
Commit(s) ==
    /\ phase[s] = IF ~Resurrect THEN "active" ELSE IF SplitUntomb /\ LateUntomb THEN "late" ELSE "checked"
    /\ \A r \in records : (r.kind = "copy" /\ r.owner = s /\ r.obj \notin dedup[s]) => Children(r.obj) \subseteq refs[s]
    /\ phase' = [phase EXCEPT ![s] = "committed"]
    /\ records' = {IF r.owner = s THEN [r EXCEPT !.owner = None] ELSE r : r \in records}
    /\ roots' = roots \cup {<<s, o>> : o \in refs[s]}
    /\ UNCHANGED <<answer, moved, clock, refs, gc, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* a session ends without committing, by choice or because the reaper ended
\* it after a crash; its end marker keeps it from ever committing
Abort(s) ==
    /\ phase[s] \in {"begun", "active", "flushed", "untombed", "checked", "late"}
    /\ phase' = [phase EXCEPT ![s] = "aborted"]
    /\ records' = {r \in records : r.owner # s}
    /\ UNCHANGED <<answer, moved, clock, refs, roots, gc, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* a session stores the record of a commit the store goes on to refuse
Try(s, c) ==
    /\ phase[s] = "active"
    /\ answer[c] = "none"
    /\ answer' = [answer EXCEPT ![c] = s]
    /\ \E v \in NewVersions :
         /\ records' = records \cup {Record("copy", c, v, s, s)}
         /\ clock' = IF v > clock THEN v ELSE clock
    /\ UNCHANGED <<moved, phase, refs, roots, gc, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* the store answers the commit refused and tombstones its record: in the
\* session while it may still commit, in a committed archive once it has;
\* a session that aborted took the record with it
Refuse(s, c) ==
    /\ answer[c] = s
    /\ answer' = [answer EXCEPT ![c] = "refused"]
    /\ IF phase[s] = "aborted" \/ (phase[s] = "committed" /\ SkipCommittedRefusal)
         THEN UNCHANGED <<records, clock>>
         ELSE \E r \in records, v \in NewVersions :
                /\ r.kind = "copy" /\ r.obj = c /\ r.ver < v
                /\ records' = records \cup {Record("tomb", c, v, s, IF phase[s] = "committed" THEN None ELSE s)}
                /\ clock' = IF v > clock THEN v ELSE clock
    /\ UNCHANGED <<moved, phase, refs, roots, gc, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* the tombstone of a refusal cannot be stored while the session may still
\* commit, so the session ends and takes the record with it
LoseRefusal(s, c) ==
    /\ answer[c] = s
    /\ ~Ended(s)
    /\ answer' = [answer EXCEPT ![c] = "refused"]
    /\ IF KeepRefusingSession
         THEN UNCHANGED <<phase, records>>
         ELSE /\ phase' = [phase EXCEPT ![s] = "aborted"]
              /\ records' = {r \in records : r.owner # s}
    /\ UNCHANGED <<moved, clock, refs, roots, gc, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* a backup is deleted, so what only it referenced becomes unreachable
Retire(p) ==
    /\ roots' = roots \ {p}
    /\ retired' = retired \cup {p}
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, gc, lease, cycles, dedup, sealed, plans, rw, state, seen>>

\* the revival is written under the collector's lock, after a check that
\* every object the backup reaches has a committed copy; the next mark
\* takes it as a root
Unretire(p) ==
    /\ Unretiring
    /\ p \in retired
    /\ \/ SkipReviveLock
       \/ /\ \A m \in Maintainers : gc[m].pc = "idle"
          /\ \A w \in Rewriters : rw[w].pc = "idle"
          /\ plans = {}
    /\ SkipReviveCheck \/ \A o \in Closure({p[2]}) : \E c \in Committed : c.kind = "copy" /\ c.obj = o
    /\ roots' = roots \cup {p}
    /\ retired' = retired \ {p}
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, gc, lease, cycles, dedup, sealed, plans, rw, state, seen>>

(* the maintainers: one holds the lease at a time, but a maintainer whose
   lease ran out may not know it yet *)

\* every step re-checks the lease, except the deletes that follow the last
\* check
Holds(m) == NoFence \/ lease = m

Acquire(m) ==
    /\ lease = None
    /\ gc[m].pc = "idle"
    /\ cycles < MaxCycles
    /\ lease' = m
    /\ cycles' = cycles + 1
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, gc, dedup, sealed, plans, rw, state, seen, retired>>

\* the holder stalls past its lease
Expire ==
    /\ lease # None
    /\ lease' = None
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, gc, cycles, dedup, sealed, plans, rw, state, seen, retired>>

Mark(m) ==
    /\ gc[m].pc = "idle"
    /\ Holds(m)
    /\ SkipPlanWait \/ plans = {}
    /\ gc' = [gc EXCEPT ![m].pc = "marked", ![m].m1 = Reachable,
                        ![m].snap = {c \in Committed : c.kind = "copy"}]
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* one tombstone archive for every unmarked object with a copy in the snapshot
Condemn(m) ==
    LET doomed == {o \in Objects \ gc[m].m1 : \E r \in gc[m].snap : r.obj = o}
    IN /\ gc[m].pc = "marked"
       /\ Holds(m)
       /\ \E ver \in [doomed -> NewVersions] :
            /\ records' = records \cup {Record("tomb", o, ver[o], m, None) : o \in doomed}
            /\ moved' = moved \ {Record("tomb", o, ver[o], m, None) : o \in doomed}
            /\ clock' = IF doomed = {} THEN clock ELSE
                          LET top == CHOOSE v \in {ver[o] : o \in doomed} : \A o \in doomed : ver[o] <= v
                          IN IF top > clock THEN top ELSE clock
       /\ gc' = [gc EXCEPT ![m].pc = "condemned"]
       /\ UNCHANGED <<answer, phase, refs, roots, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* after the tombstones are written, list the sessions that might have
\* deduplicated before them; every tombstone in the store is handled
Horizon(m) ==
    /\ gc[m].pc = "condemned"
    /\ Holds(m)
    /\ gc' = [gc EXCEPT ![m].pc = "listed",
                        ![m].older = {s \in Sessions : ~Ended(s)},
                        ![m].tombs = {r \in Committed : r.kind = "tomb"},
                        ![m].untombs = {r \in Committed : r.kind = "untomb"}]
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* sessions check the seal after writing their un-tombstones, and the
\* maintainer reads un-tombstones after sealing: one side sees the other
Seal(m) ==
    /\ Resurrect
    /\ gc[m].pc = "listed"
    /\ Holds(m)
    /\ sealed' = sealed \cup gc[m].tombs
    /\ gc' = [gc EXCEPT ![m].pc = "sealed", ![m].live = {s \in Sessions : ~Ended(s)}]
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, lease, cycles, dedup, plans, rw, state, seen, retired>>

Confirm(m) ==
    /\ gc[m].pc = IF Resurrect THEN "sealed" ELSE "listed"
    /\ Holds(m)
    /\ SkipWait \/ Resurrect \/ \A s \in gc[m].older : Ended(s)
    /\ gc' = [gc EXCEPT ![m].pc = "confirmed", ![m].m2 = IF SkipConfirm THEN gc[m].m1 ELSE Reachable]
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* pick the snapshot copies older than a tombstone of an object still
\* unmarked, and the tombstones whose object is marked again or has no older
\* copy left
Plan(m) ==
    LET g == gc[m]
        tombs == g.tombs \cap records
        untombs == IF EarlyUntombs THEN g.untombs \cap records ELSE {r \in records : r.kind = "untomb"}
        \* what a live session's un-tombstones take back is a root
        kept == IF SkipClosure THEN {} ELSE Closure({u.obj : u \in {v \in untombs : v.writer \in g.live}})
        dead == {c \in records :
                    /\ SkipSnapshot \/ c \in g.snap
                    /\ c.kind = "copy" /\ c.owner = None /\ c.obj \notin g.m2 \cup kept
                    /\ \E t \in tombs : t.obj = c.obj /\ c.ver < t.ver /\ ~Revoked(t, untombs, g.live)}
        spent == {t \in tombs :
                    \/ t.obj \in g.m2
                    \/ Revoked(t, untombs, g.live)
                    \/ ~\E c \in records \ dead : c.kind = "copy" /\ c.obj = t.obj /\ c.ver < t.ver}
        \* an un-tombstone goes once its session ended and no tombstone of
        \* its object is left
        idle == {u \in untombs :
                    /\ Ended(u.writer)
                    /\ ~\E t \in {r \in records : r.kind = "tomb"} \ spent : t.obj = u.obj}
    IN /\ g.pc = "confirmed"
       /\ Holds(m)
       /\ gc' = [gc EXCEPT ![m].pc = "planned", ![m].drop = dead \cup spent \cup idle]
       /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

Delete(m) ==
    /\ ~Handoff
    /\ gc[m].pc = "planned"
    /\ records' = records \ gc[m].drop
    /\ gc' = [gc EXCEPT ![m] = Idle]
    /\ lease' = IF lease = m THEN None ELSE lease
    /\ UNCHANGED <<answer, moved, clock, phase, refs, roots, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* the plan goes to the bucket, and the lease is let go for a rewriter
Publish(m) ==
    /\ Handoff
    /\ gc[m].pc = "planned"
    /\ Holds(m)
    /\ plans' = plans \cup {gc[m].drop}
    /\ gc' = [gc EXCEPT ![m] = Idle]
    /\ lease' = IF lease = m THEN None ELSE lease
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, cycles, dedup, sealed, rw, state, seen, retired>>

TakePlan(w, plan) ==
    /\ rw[w].pc = "idle"
    /\ lease = None
    /\ cycles < MaxCycles
    /\ plan \in plans
    /\ lease' = w
    /\ cycles' = cycles + 1
    /\ rw' = [rw EXCEPT ![w] = [pc |-> "loaded", plan |-> plan]]
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, gc, dedup, sealed, plans, state, seen, retired>>

\* the deletes follow the last lease check, as a maintainer's do
Rewrite(w) ==
    /\ rw[w].pc = "loaded"
    /\ records' = records \ rw[w].plan
    /\ rw' = [rw EXCEPT ![w].pc = "rewritten"]
    /\ UNCHANGED <<answer, moved, clock, phase, refs, roots, gc, lease, cycles, dedup, sealed, plans, state, seen, retired>>

\* a rewriter that dies before this leaves the plan to be run again
Finish(w) ==
    /\ rw[w].pc = "rewritten"
    /\ plans' = plans \ {rw[w].plan}
    /\ rw' = [rw EXCEPT ![w] = [pc |-> "idle", plan |-> {}]]
    /\ lease' = IF lease = w THEN None ELSE lease
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, gc, cycles, dedup, sealed, state, seen, retired>>

RewriterCrash(w) ==
    /\ rw[w].pc # "idle"
    /\ rw' = [rw EXCEPT ![w] = [pc |-> "idle", plan |-> {}]]
    /\ lease' = IF lease = w THEN None ELSE lease
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, gc, cycles, dedup, sealed, plans, state, seen, retired>>

(* pipelined generations: a mark confirms what the previous generation
   condemned and condemns for the next; un-tombstones carry no writer, so
   every one keeps what it takes back *)

\* un-tombstones stay their session's until it commits, so no other session
\* relies on them before its copies are made; marks keep what they take back
\* from the moment they are written
Untombs == {r \in IF SkipPendingRoots THEN Committed ELSE records : r.kind = "untomb"}

PMark(m) ==
    LET kept == IF SkipUntombRoots THEN {} ELSE Closure({u.obj : u \in Untombs})
    IN /\ Pipelined
       /\ gc[m].pc = "idle"
       /\ Holds(m)
       /\ SkipPlanWait \/ plans = {}
       /\ gc' = [gc EXCEPT ![m].pc = "marked", ![m].m1 = Reachable \cup kept,
                           ![m].snap = {c \in Committed : c.kind = "copy"},
                           ![m].untombs = Untombs]
       /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* drop the previous snapshot's copies both marks left unmarked that a
\* tombstone of the previous horizon condemns and no un-tombstone takes
\* back; tombstones go once taken back or with no older copy left;
\* un-tombstones go once the previous horizon's sessions ended and no
\* tombstone of theirs is left
PPlan(m) ==
    LET g == gc[m]
        tombs == state.tombs \cap records
        dead == {c \in records :
                    /\ c \in state.snap
                    /\ c.kind = "copy" /\ c.owner = None
                    /\ c.obj \notin g.m1 \cup state.m1
                    /\ \E t \in tombs : t.obj = c.obj /\ c.ver < t.ver
                    /\ ~\E u \in g.untombs : u.obj = c.obj}
        \* a copy as old as the tombstone is hidden by it, as Has decides a tie
        spent == {t \in tombs :
                    \/ \E u \in g.untombs : u.obj = t.obj
                    \/ ~\E c \in records : c.kind = "copy" /\ c.obj = t.obj /\ c.ver <= t.ver}
        idle == {u \in state.untombs \cap records :
                    /\ \A s \in state.live : Ended(s)
                    /\ ~\E t \in records : t.kind = "tomb" /\ t.obj = u.obj}
    IN /\ g.pc = "marked"
       /\ Holds(m)
       /\ gc' = [gc EXCEPT ![m].pc = "planned", ![m].drop = IF state.valid THEN dead \cup spent \cup idle ELSE {}]
       /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

PCondemn(m) ==
    LET doomed == {o \in Objects \ gc[m].m1 : \E r \in gc[m].snap : r.obj = o}
    IN /\ gc[m].pc = "planned"
       /\ Holds(m)
       /\ \E ver \in [doomed -> NewVersions] :
            /\ TombsNewer => \A o \in doomed : \A c \in gc[m].snap : c.obj = o => c.ver < ver[o]
            /\ records' = records \cup {Record("tomb", o, ver[o], m, None) : o \in doomed}
            /\ moved' = moved \ {Record("tomb", o, ver[o], m, None) : o \in doomed}
            /\ clock' = IF doomed = {} THEN clock ELSE
                          LET top == CHOOSE v \in {ver[o] : o \in doomed} : \A o \in doomed : ver[o] <= v
                          IN IF top > clock THEN top ELSE clock
       /\ gc' = [gc EXCEPT ![m].pc = "condemned"]
       /\ UNCHANGED <<answer, phase, refs, roots, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* every tombstone the store holds was stored before this horizon, wherever
\* compaction moved it
PHorizon(m) ==
    LET stood == {r \in Committed : r.kind = "tomb"}
    IN /\ gc[m].pc = "condemned"
       /\ Holds(m)
       /\ gc' = [gc EXCEPT ![m].pc = "listed",
                           ![m].tombs = IF SkipCarried THEN {r \in stood : r \notin moved \/ r \in state.tombs} ELSE stood,
                           ![m].stood = stood,
                           ![m].older = Untombs]
       /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* the seal and the generation's record are written after the sessions are
\* listed
PSeal(m) ==
    /\ gc[m].pc = "listed"
    /\ Holds(m)
    /\ sealed' = sealed \cup gc[m].tombs
    /\ state' = [valid |-> TRUE, snap |-> gc[m].snap, m1 |-> gc[m].m1, tombs |-> gc[m].tombs,
                 untombs |-> gc[m].older, live |-> {s \in Sessions : ~Ended(s)}, stood |-> gc[m].stood]
    /\ gc' = [gc EXCEPT ![m].pc = "sealed"]
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, lease, cycles, dedup, plans, rw, seen, retired>>

PDelete(m) ==
    /\ ~Handoff
    /\ gc[m].pc = "sealed"
    /\ records' = records \ gc[m].drop
    /\ gc' = [gc EXCEPT ![m] = Idle]
    /\ lease' = IF lease = m THEN None ELSE lease
    /\ UNCHANGED <<answer, moved, clock, phase, refs, roots, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* the plan goes to the bucket for a rewriter, as Publish does
PPublish(m) ==
    /\ Handoff
    /\ gc[m].pc = "sealed"
    /\ Holds(m)
    /\ plans' = plans \cup {gc[m].drop}
    /\ gc' = [gc EXCEPT ![m] = Idle]
    /\ lease' = IF lease = m THEN None ELSE lease
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, cycles, dedup, sealed, rw, state, seen, retired>>

\* a rewrite moves a tombstone into a new archive with the version it had
Compact(t) ==
    /\ Compaction
    /\ t \in Committed \ moved
    /\ t.kind = "tomb"
    /\ moved' = moved \cup {t}
    /\ UNCHANGED <<answer, records, clock, phase, refs, roots, gc, lease, cycles, dedup, sealed, plans, rw, state, seen, retired>>

\* a maintainer dies anywhere; what it wrote stays, its lease runs out
Crash(m) ==
    /\ gc[m].pc # "idle"
    /\ gc' = [gc EXCEPT ![m] = Idle]
    /\ lease' = IF lease = m THEN None ELSE lease
    /\ UNCHANGED <<answer, moved, records, clock, phase, refs, roots, cycles, dedup, sealed, plans, rw, state, seen, retired>>

Next ==
    \/ \E s \in Sessions : Begin(s) \/ Untomb(s) \/ CommitUntomb(s) \/ TombedUntomb(s) \/ Check(s) \/ Commit(s) \/ Abort(s)
    \/ \E s \in Sessions : Recheck(s)
    \/ \E s \in Sessions, o \in Objects : Use(s, o) \/ Assume(s, o)
    \/ \E s \in Sessions, c \in Tries : Try(s, c) \/ Refuse(s, c) \/ LoseRefusal(s, c)
    \/ \E p \in roots : Retire(p)
    \/ \E p \in retired : Unretire(p)
    \/ Expire
    \/ \E t \in records : Compact(t)
    \/ \E m \in Maintainers :
         \/ Acquire(m) \/ Crash(m)
         \/ ~Pipelined /\ (Mark(m) \/ Condemn(m) \/ Horizon(m) \/ Seal(m) \/ Confirm(m) \/ Plan(m) \/ Delete(m) \/ Publish(m))
         \/ PMark(m) \/ PPlan(m) \/ PCondemn(m) \/ PHorizon(m) \/ PSeal(m) \/ PDelete(m) \/ PPublish(m)
    \/ \E w \in Rewriters : Rewrite(w) \/ Finish(w) \/ RewriterCrash(w) \/ \E plan \in plans : TakePlan(w, plan)

Spec == Init /\ [][Next]_vars

(* properties *)

Safe == \A o \in Reachable : \E r \in Committed : r.kind = "copy" /\ r.obj = o

\* the next generation condemns with every tombstone that stood at the last
\* horizon and stands still
Collectable == state.stood \cap records \subseteq state.tombs

Refused ==
    \A c \in Tries : answer[c] = "refused" =>
        \A r \in Committed : (r.kind = "copy" /\ r.obj = c) =>
            \E t \in Committed : t.kind = "tomb" /\ t.obj = c /\ r.ver < t.ver

TypeOK ==
    /\ phase \in [Sessions -> {"new", "begun", "active", "flushed", "untombed", "checked", "late", "committed", "aborted"}]
    /\ refs \in [Sessions -> SUBSET Objects]
    /\ roots \subseteq Sessions \X Objects
    /\ answer \in [Tries -> Sessions \cup {"none", "refused"}]
=============================================================================
