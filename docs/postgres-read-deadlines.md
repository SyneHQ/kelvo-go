# PostgreSQL read deadlines

Kelvo starts a read-only transaction and installs a transaction-local `statement_timeout` before each PostgreSQL query or metadata read. It uses the remaining operation budget, capped by the read path's 30-second limit. An expired or sub-millisecond budget, or a failed timeout setting, prevents query dispatch.

The server timer survives an adapter process crash. It is an additional bound, not proof of source termination: cancellation still needs the original connection's completion evidence and physical cleanup. Unknown outcomes remain `cleanup_unknown`.

PostgreSQL applies this relative timer when the source statement starts. Network and setup time can shift its wall-clock end relative to the caller's deadline. The Go context remains responsible for normal transport cancellation.

A read-only transaction does not make arbitrary source functions harmless. Functions can have external effects or change session settings; operators must restrict the source role's function privileges. This timer does not prove that external effects stopped or were rolled back.
