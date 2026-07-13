# Incident Log

This file lists real problems found while running this pipeline, how they were found, and how they were fixed. Writing this down was as useful as fixing the bugs — most of these are common Airflow and Docker problems, so it may help others too.

---

## 1. Task hangs silently, no error in logs (OOM)

**Symptom:** `process_tweets` task would sometimes just stop logging for several minutes, then get killed by Airflow as a "zombie task" — no Python error, no traceback.

**Root cause:** The sentiment model was loaded at the top of the DAG file, outside any function. In Airflow, this code runs every time the file is parsed — which means:
- The **scheduler** loaded the ML model just to read the DAG structure, even though it never needed the model.
- **Every task** in the DAG (even ones that don't use the model) loaded it too.

This repeated, unnecessary loading used a lot of memory and was the likely cause of the process getting killed.

**Fix:** Changed to lazy loading — the model is loaded only once, only inside the task that actually needs it (`process_tweets`), on its first real use. Used a simple cache (module-level variable) so it's not reloaded on every tweet.

**Lesson:** Never do heavy work (loading models, opening real connections) at the top level of an Airflow DAG file. That code runs constantly, not just when a task executes.

---

## 2. Scheduler gets stuck, DAGs disappear and reappear

**Symptom:** Scheduler logs showed:
```
DAG sahamyab_sentiment_analysis is missing and will be deactivated.
Killing DAGFileProcessorProcess (PID=...)
```
DAGs would vanish from the UI, then come back a bit later. "Next run" times looked wrong.

**Root cause:** `import torch` and `from transformers import ...` were still at the top of the file. Even without loading the model, just importing these libraries is slow (can take many seconds). The scheduler re-parses every DAG file constantly (default: every ~30 seconds) to detect changes — and each parse paid this import cost. When it took too long, Airflow's internal timeout killed the parsing process, which looked like the DAG "disappearing."

**Fix:** Moved `import torch` and `from transformers import ...` inside the function that uses them, instead of the top of the file. Now parsing the file is fast (it doesn't need these libraries at all), and the import only happens once, inside the real task process.

**Lesson:** Heavy imports belong close to where they're used, not at the top of a DAG file — same idea as lesson #1, just about imports instead of model loading.

---

## 3. Zombie tasks after laptop sleep

**Symptom:** A task would start, log a few lines, then go silent for hours, then Airflow would mark it as a "zombie" and kill it.

**Root cause:** The laptop (or Docker Desktop) went to sleep while a task was running. All containers freeze during sleep. When the laptop woke up, the frozen process resumed exactly where it left off — but from Airflow's point of view, it hadn't sent a heartbeat in hours, so it was treated as dead.

One log line made this very clear — a task duration showed as **negative** (`-37834.77s`), which is only possible if the system clock froze and then jumped during a sleep/resume cycle.

**Fix:**
- Added `execution_timeout` to the DAG's `default_args`, so a task gets stopped and retried after a fixed time instead of waiting indefinitely.
- Increased `retries` so a one-off freeze doesn't need manual fixing.
- Switched internal timing code from `time.time()` (wall clock, breaks across sleep/resume) to `time.monotonic()` (a clock that only moves forward, safe for measuring durations).

**Lesson:** On a machine that can sleep, always assume a task can be paused mid-execution. Timeouts and retries aren't optional — they're how the pipeline survives that.

---

## 4. "Execution date is in the future" errors

**Symptom:** Airflow refused to run tasks, saying their scheduled time was in the future — even though that time had already passed in the real world. The next scheduled run shown in the UI didn't match the real current time at all.

**Root cause:** Two things stacked together:
1. The Windows laptop clock was set manually, without automatic time sync.
2. After the laptop slept and woke up, the WSL2 (Docker's Linux environment on Windows) clock drifted out of sync with real time.

Since Airflow trusts the container's clock completely, a wrong clock means wrong scheduling decisions everywhere.

**Fix:**
1. Turned on automatic time sync in Windows (`Settings → Time & Language → Set time automatically`), and forced an immediate resync with `w32tm /resync /force`.
2. Ran `wsl --shutdown` to force WSL2 to fully restart and re-sync its clock from the (now correct) host clock.
3. Cleared the stale scheduling state in Airflow's database so it would recompute the next run from the correct current time.

**Lesson:** Airflow scheduling is only as reliable as the system clock underneath it. On a laptop that sleeps, clock drift is a real risk — automatic time sync is not optional for something meant to run on a schedule.

---

## 5. Docker Desktop won't open after sleep

**Symptom:** After the laptop woke up from sleep, Docker Desktop simply wouldn't open.

**Root cause:** The abrupt sleep/resume left the WSL2 virtual machine (which Docker Desktop depends on) in a broken state.

**Fix:** `wsl --shutdown` to fully tear down the WSL2 VM, then reopening Docker Desktop, which recreated it cleanly. A full Windows restart also resolved it when the above wasn't enough.

**Lesson:** Most "Docker Desktop is broken" problems on Windows are actually WSL2 problems. `wsl --shutdown` is usually the fastest fix.

---

## Summary table

| # | Symptom | Root cause | Fix |
|---|---|---|---|
| 1 | Silent task hang, killed as zombie | Model loaded at DAG-parse time | Lazy-load model inside the task |
| 2 | DAGs disappear/reappear in UI | Heavy library imports at top of file | Move imports inside functions |
| 3 | Zombie task, negative durations in logs | Laptop/Docker sleep froze the container | `execution_timeout`, retries, `time.monotonic()` |
| 4 | "Execution date in future" errors | Manual clock + WSL2 clock drift after sleep | Enable auto time sync, `wsl --shutdown` |
| 5 | Docker Desktop won't open | WSL2 VM corrupted by abrupt sleep | `wsl --shutdown`, or full restart |
