import assert from "node:assert/strict";
import http from "node:http";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createInterface } from "node:readline";
import { chromium } from "playwright";

const repository = new URL("../..", import.meta.url).pathname;
const server = spawn(
  "cargo",
  ["run", "--quiet", "--manifest-path", "interop/Cargo.toml", "--bin", "chromium_server"],
  { cwd: repository, stdio: ["ignore", "pipe", "inherit"] },
);

const lines = createInterface({ input: server.stdout });
const [ready] = await once(lines, "line");
const match = /^READY (\d+) ([0-9a-f]{64})$/.exec(ready);
assert.ok(match, `unexpected server readiness line: ${ready}`);
const [, port, fingerprint] = match;
const certificateHash = Uint8Array.from(
  fingerprint.match(/../g).map((byte) => Number.parseInt(byte, 16)),
);

const pageServer = http.createServer((_request, response) => {
  response.writeHead(200, { "content-type": "text/html" });
  response.end("<!doctype html><title>WebTransport interoperability</title>");
});
pageServer.listen(0, "127.0.0.1");
await once(pageServer, "listening");
const pagePort = pageServer.address().port;

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage();
await page.goto(`http://127.0.0.1:${pagePort}`);

const options = {
  base: `https://127.0.0.1:${port}`,
  certificateHash: Array.from(certificateHash),
};

async function exerciseEcho(page, testOptions) {
  return page.evaluate(async ({ base, certificateHash }) => {
    const transport = new WebTransport(`${base}/echo`, {
      allowPooling: false,
      serverCertificateHashes: [
        {
          algorithm: "sha-256",
          value: Uint8Array.from(certificateHash),
        },
      ],
    });
    await transport.ready;

    const bi = await transport.createBidirectionalStream();
    const biWriter = bi.writable.getWriter();
    await biWriter.write(new TextEncoder().encode("bidirectional"));
    await biWriter.close();
    const biReply = await new Response(bi.readable).arrayBuffer();
    if (new TextDecoder().decode(biReply) !== "bidirectional") {
      throw new Error("bidirectional stream echo mismatch");
    }

    const uni = await transport.createUnidirectionalStream();
    const uniWriter = uni.getWriter();
    await uniWriter.write(new TextEncoder().encode("unidirectional"));
    await uniWriter.close();
    const incomingUni = await transport.incomingUnidirectionalStreams
      .getReader()
      .read();
    const uniReply = await new Response(incomingUni.value).arrayBuffer();
    if (new TextDecoder().decode(uniReply) !== "unidirectional") {
      throw new Error("unidirectional stream echo mismatch");
    }

    const datagramWriter = transport.datagrams.writable.getWriter();
    await datagramWriter.write(new TextEncoder().encode("datagram"));
    const datagram = await transport.datagrams.readable.getReader().read();
    if (new TextDecoder().decode(datagram.value) !== "datagram") {
      throw new Error("datagram echo mismatch");
    }

    transport.close({ closeCode: 0x10203040, reason: "browser close" });
    await new Promise((resolve) => setTimeout(resolve, 100));
  }, testOptions);
}

try {
  await exerciseEcho(page, options);

  const closeInfo = await page.evaluate(async ({ base, certificateHash }) => {
    const transport = new WebTransport(`${base}/server-close`, {
      allowPooling: false,
      serverCertificateHashes: [
        {
          algorithm: "sha-256",
          value: Uint8Array.from(certificateHash),
        },
      ],
    });
    await transport.ready;
    return transport.closed;
  }, options);
  assert.deepEqual(closeInfo, {
    closeCode: 0xfedcba98,
    reason: "native close",
  });

  const rejected = await page.evaluate(async ({ base, certificateHash }) => {
    const transport = new WebTransport(`${base}/reject`, {
      allowPooling: false,
      serverCertificateHashes: [
        {
          algorithm: "sha-256",
          value: Uint8Array.from(certificateHash),
        },
      ],
    });
    try {
      await transport.ready;
      return false;
    } catch {
      return true;
    }
  }, options);
  assert.equal(rejected, true, "Chromium accepted a rejected request");

  await exerciseEcho(page, options);
} finally {
  await browser.close();
  pageServer.close();
}

const exitCode = server.exitCode ?? (await once(server, "exit"))[0];
assert.equal(exitCode, 0, "Chromium interoperability server failed");
