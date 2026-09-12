import { cp, mkdir, readFile, rm } from "node:fs/promises";
import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const repository = dirname(dirname(fileURLToPath(import.meta.url)));
const site = join(repository, "site");
const output = join(site, "dist");

function run(program, args, cwd, env = process.env) {
  return new Promise((resolvePromise, reject) => {
    const child = spawn(program, args, { cwd, env, stdio: "inherit" });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0) resolvePromise();
      else reject(new Error(`${program} exited with ${code ?? signal}`));
    });
  });
}

await run("cargo", ["doc", "--all-features", "--no-deps"], repository);
await run("bun", ["run", "build:site"], site);

const apiOutput = join(output, "api");
await rm(apiOutput, { recursive: true, force: true });
await cp(join(repository, "target", "doc"), apiOutput, { recursive: true });

const registry = await readFile(join(site, "src", "content", "docs.ts"), "utf8");
const slugs = Array.from(registry.matchAll(/slug:\s*"([^"]+)"/g), (match) => match[1]);
const index = join(output, "index.html");
for (const route of ["docs", ...slugs.map((slug) => `docs/${slug}`)]) {
  const routeDirectory = join(output, route);
  await mkdir(routeDirectory, { recursive: true });
  await cp(index, join(routeDirectory, "index.html"));
}
await cp(index, join(output, "404.html"));

console.log(`Built portal, ${slugs.length} documentation routes, and generated Rust API reference.`);
