const types = {
  feat: "feature",
  fix: "bug",
  refactor: "refactor",
  docs: "docs",
  test: "test",
  ci: "ci",
  chore: "chore",
  build: "chore",
  perf: "performance",
  style: "chore",
  revert: "revert",
};
const areas = [
  "runtime",
  "profiles",
  "security",
  "client",
  "ci",
  "docs",
  "tests",
  "image",
];
const sizes = ["XS", "S", "M", "L", "XL"];
export const labels = new Map([
  ...areas.map((area) => [`area/${area}`, "F59E0B"]),
  ...[...new Set(Object.values(types))].map((type) => [`type/${type}`, "2563EB"]),
  ...sizes.map((size) => [`size:${size}`, "65A30D"]),
]);

export function classify(title, files) {
  const desired = new Set();
  const type = title.match(/^(\w+)(?:\([^)]*\))?!?:/i)?.[1].toLowerCase();
  if (types[type]) desired.add(`type/${types[type]}`);
  let total = 0;
  for (const file of files) {
    for (const path of [file.filename, file.previous_filename].filter(Boolean)) {
      if (path.startsWith("src/")) desired.add("area/runtime");
      if (
        /^src\/(?:profiles|local-profiles|profile-work|cookies|archive-channel|files)\.ts$/.test(
          path,
        )
      )
        desired.add("area/profiles");
      if (
        /^src\/(?:peer-identity|network-policy|privacy|prepare-seccomp|takeover)\.ts$/.test(
          path,
        ) ||
        path.startsWith("config/")
      )
        desired.add("area/security");
      if (
        /^src\/(?:host-client|session-cli|session|cdp)\.ts$/.test(path) ||
        path.startsWith("examples/")
      )
        desired.add("area/client");
      if (path.startsWith(".github/")) desired.add("area/ci");
      if (path.startsWith("docs/") || /^[^/]+\.md$/i.test(path))
        desired.add("area/docs");
      if (path.startsWith("tests/")) desired.add("area/tests");
      if (/^Dockerfile(?:\.|$)/.test(path) || path === ".dockerignore")
        desired.add("area/image");
    }
    if (!/(^|\/)(?:pnpm-lock\.yaml|dist\/)|\.snap$/.test(file.filename))
      total += file.additions + file.deletions;
  }
  desired.add(
    `size:${sizes[[20, 100, 300, 1000].filter((limit) => total >= limit).length]}`,
  );
  return [...desired];
}

export default async function labelPr({ github, context, core }) {
  const { owner, repo } = context.repo;
  const existing = await github.paginate(github.rest.issues.listLabelsForRepo, {
    owner,
    repo,
    per_page: 100,
  });
  for (const [name, color] of labels) {
    if (!existing.some((label) => label.name === name)) {
      try {
        await github.rest.issues.createLabel({ owner, repo, name, color });
      } catch (error) {
        if (
          error.status !== 422 ||
          !error.response?.data?.errors?.some(
            (entry) => entry.code === "already_exists",
          )
        )
          throw error;
      }
    }
  }
  const input =
    context.payload.inputs?.pr ?? String(context.payload.pull_request?.number);
  if (input !== "all" && !/^[1-9]\d*$/.test(input))
    throw Error("Provide a PR number or all");
  const prs =
    input === "all"
      ? await github.paginate(github.rest.pulls.list, {
          owner,
          repo,
          state: "open",
          base: "master",
          per_page: 100,
        })
      : [{ number: Number(input) }];
  for (const { number } of prs) {
    const { data: pr } = await github.rest.pulls.get({
      owner,
      repo,
      pull_number: number,
    });
    if (pr.state !== "open" || pr.base.ref !== "master") continue;
    const files = await github.paginate(github.rest.pulls.listFiles, {
      owner,
      repo,
      pull_number: number,
      per_page: 100,
    });
    // GitHub caps this endpoint at 3,000 files; do not publish partial classifications.
    if (files.length !== pr.changed_files)
      throw Error(`Incomplete file list for PR #${number}`);
    const desired = classify(pr.title, files);
    const current = pr.labels.map((label) => label.name);
    const add = desired.filter((label) => !current.includes(label));
    if (add.length)
      await github.rest.issues.addLabels({
        owner,
        repo,
        issue_number: number,
        labels: add,
      });
    for (const name of current.filter(
      (label) => labels.has(label) && !desired.includes(label),
    )) {
      await github.rest.issues
        .removeLabel({ owner, repo, issue_number: number, name })
        .catch((error) => {
          if (error.status !== 404) throw error;
        });
    }
    core.info(`Updated labels for PR #${number}`);
  }
}
