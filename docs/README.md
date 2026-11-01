# Yowza! Documentation

Welcome to the official documentation for Yowza!, the edge-native, serverless-first database framework.

## Introduction
Yowza! is designed from the ground up to solve the challenges of running stateful applications in ephemeral environments, such as AWS Lambda, Cloudflare Workers, and edge nodes. By combining a highly optimized C core with WebAssembly portability and an idiomatic Go SDK, Yowza! provides ultra-fast startups, zero-copy memory access, and robust cloud synchronization.

## Documentation Structure
- **Concepts:** Learn about the core philosophy and architecture of Yowza!
- **Core Engine:** Deep dives into the WAL, ART index, memory arena, and OpCode VM.
- **Adapters:** Guides on using Yowza! for specific use cases, such as serverless Telegram bots.
- **Cloud:** Understand how Yowza! synchronizes data across the edge and achieves consensus.
- **Security:** Details on our Ed25519 authentication and ChaCha20-Poly1305 encryption.
- **Deployment:** Instructions on running Yowza! natively, as a daemon, or in WebAssembly.
- **RFCs:** Historical design documents tracking the evolution of the engine.
