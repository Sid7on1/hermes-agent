---
title: Hermes Agenta SLAM
emoji: 🤖
colorFrom: indigo
colorTo: purple
sdk: docker
pinned: false
app_port: 7860
---

# 🤖 Hermes AI Agent (with Persistent Memory & Adaptive Key Rotation)

This is the production-grade deployment configuration for the Hermes background Telegram Agent, optimized to run 24/7 on Hugging Face Spaces using the CPU Basic tier (16 GB RAM, 2 vCPUs).

## Key Capabilities:
* **Background Watchdog**: Auto-restarts Hermes on unexpected crashes, guards against memory leaks, and checks for Telegram double-polling conflicts.
* **Supabase Incremental Sync**: Restores your state database, memories, skills, and prompts instantly on boot and uploads changes periodically.
* **NVIDIA NIM Key Proxy**: Automatically rotates 6 active NIM keys with thread-safe exponential backoffs.
