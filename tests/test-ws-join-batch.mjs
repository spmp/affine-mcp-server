#!/usr/bin/env node
// AFFiNE >= 0.27.5 (and canary since 2026-09) replaced `space:join` with `space:join-batch` and only lets a
// socket load or push a document it joined by docId. Older servers only know `space:join`.
import assert from "node:assert/strict";

import { joinWorkspace, loadDoc, pushDocUpdate } from "../dist/ws.js";

const WORKSPACE = "ws-1";

class FakeServerSocket {
  constructor(protocol) {
    this.protocol = protocol; // "legacy" | "batch"
    this.joinedDocs = new Set();
    this.joinedSpace = false;
    this.events = [];
    this.existing = new Set([WORKSPACE, "doc-a"]);
    this.accepted = [];
  }

  emit(event, payload, ack) {
    this.events.push({ event, payload });
    if (this.protocol === "legacy") {
      if (event === "space:join") { this.joinedSpace = true; return ack({ data: { clientId: "c" } }); }
      if (event === "space:join-batch") return; // unknown event: no ack, like socket.io/NestJS
      if (!this.joinedSpace) return ack({ error: { name: "NOT_IN_SPACE", message: "not in space" } });
      return this.handleDoc(event, payload, ack);
    }
    if (event === "space:join") return; // removed in the batch protocol: no ack
    if (event === "space:join-batch") {
      assert.ok(payload.clientVersion, "batch join needs a client version");
      // canary 2026-11-24 rejects joins without a sync protocol version ("Sync protocol upgrade required.")
      assert.ok([1, 2].includes(payload.protocolVersion), "batch join needs protocolVersion 1 or 2");
      assert.ok(/^0\.(2[89]|[3-9]\d)|^0\.27\.([5-9]|\d\d)|^[1-9]/.test(payload.clientVersion),
        `client version ${payload.clientVersion} is below 0.27.5`);
      for (const space of payload.spaces) {
        assert.equal(space.spaceId, WORKSPACE);
        if (space.docId) this.joinedDocs.add(space.docId);
      }
      this.joinedSpace = true;
      return ack({ data: { clientId: "c", success: true } });
    }
    if (event === "space:push-doc-update") {
      // canary 2026-11-24: every update declares create (new doc) or update (joined, existing doc)
      if (payload.intent === "create") {
        if (this.existing.has(payload.docId)) return ack({ error: { name: "BAD_REQUEST", message: "exists" } });
        this.existing.add(payload.docId);
        this.accepted.push(`${payload.docId}:create`);
        // creating a doc bumps the workspace permission generation: the server drops every doc subscription
        this.joinedDocs.clear();
        return ack({ data: { accepted: true, timestamp: 2, created: true } });
      }
      if (payload.intent !== "update") return ack({ error: { name: "BAD_REQUEST", message: "Document update intent is required." } });
    }
    if (!this.joinedDocs.has(payload.docId)) {
      return ack({ error: { name: "NOT_IN_SPACE", message: `You should join in Space ${payload.spaceId} before broadcasting messages.` } });
    }
    if (event === "space:load-doc" && !this.existing.has(payload.docId)) {
      return ack({ error: { name: "DOC_NOT_FOUND", message: "not found" } });
    }
    return this.handleDoc(event, payload, ack);
  }

  handleDoc(event, payload, ack) {
    if (event === "space:load-doc") return ack({ data: { missing: "", state: "", timestamp: 1 } });
    if (event === "space:push-doc-update") {
      this.accepted.push(`${payload.docId}:${payload.intent ?? "legacy"}`);
      return ack({ data: { accepted: true, timestamp: 2 } });
    }
    throw new Error(`unexpected event ${event}`);
  }
}

async function exercise(protocol) {
  const socket = new FakeServerSocket(protocol);
  await joinWorkspace(socket, WORKSPACE);
  await loadDoc(socket, WORKSPACE, WORKSPACE);
  await loadDoc(socket, WORKSPACE, "doc-a");
  await pushDocUpdate(socket, WORKSPACE, "doc-b", Buffer.from([0, 0]).toString("base64")); // new doc
  await pushDocUpdate(socket, WORKSPACE, "doc-b", Buffer.from([0, 0]).toString("base64")); // now existing
  await pushDocUpdate(socket, WORKSPACE, "doc-a", Buffer.from([0, 0]).toString("base64"));
  await pushDocUpdate(socket, WORKSPACE, WORKSPACE, Buffer.from([0, 0]).toString("base64"));
  await loadDoc(socket, WORKSPACE, "doc-a");
  return socket;
}

// Legacy server: one space:join, documents need no individual join
{
  const socket = await exercise("legacy");
  assert.equal(socket.events.filter((e) => e.event === "space:join-batch" && e.payload.spaces.some((s) => s.docId && s.docId !== WORKSPACE)).length, 0,
    "a legacy server gets no per-document joins");
}

// Batch server: every document is joined once before it is used
{
  const socket = await exercise("batch");
  const joined = socket.events
    .filter((e) => e.event === "space:join-batch")
    .flatMap((e) => e.payload.spaces.map((s) => s.docId));
  assert.deepEqual([...new Set(joined)].sort(), [WORKSPACE, "doc-a", "doc-b"].sort());
  // joins repeat only after the server dropped subscriptions (doc-b's creation), never per operation
  assert.ok(joined.filter((d) => d === "doc-a").length <= 2, "doc-a is not re-joined on every operation");
  assert.deepEqual(socket.accepted, ["doc-b:create", "doc-b:update", "doc-a:update", `${WORKSPACE}:update`]);
}

console.log("ws join-batch: ok");
