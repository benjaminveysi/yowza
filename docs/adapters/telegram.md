# Building Serverless Telegram Bots

## The Challenge
Telegram bots running in serverless environments (like AWS Lambda) face strict timeouts and stateless execution. When Telegram sends an update webhook, the function spins up, processes it, and dies. If the bot needs to remember state (e.g., "User is in the middle of a signup flow"), traditional databases introduce unbearable cold-start latencies.

## The Yowza! Solution
The Yowza! Framework adapter (`adapters/telegram`) solves this by embedding the database *inside* the webhook handler.

### Idempotency Engine
Telegram often retries webhooks if the server doesn't respond fast enough. The framework uses the `UpdateID` as a primary key. Before processing any logic, Yowza! checks the index: `if DB.Get("processed:" + UpdateID)` exists, the framework immediately returns `200 OK` to Telegram, preventing duplicate execution.

### State Machine
The Framework provides a declarative `State` API.
You define states like `StateIdle`, `StateAwaitingName`. When an update arrives:
1. Yowza! retrieves the user's current state from the embedded memory: `DB.Get("user:" + ChatID + ":state")`.
2. The framework routes the update to the corresponding handler.
3. The handler mutates the state, saving it back to Yowza!.
4. The Cloud Syncer asynchronously pushes this WAL update to S3 or Redis.

On the next webhook (which might hit a completely different serverless instance), the Yowza! Manager instantly pulls the latest WAL from Redis, replays it into the Wasm memory in microseconds, and resumes the exact state.
