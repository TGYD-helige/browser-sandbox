const code = (error) =>
  /^browser_[a-z_]+$/.test(error?.message ?? "") && error.message.length <= 128
    ? error.message
    : "browser_screenshot_diagnostic_failed";

/** Diagnostic reads never convert the original failed harness command into success. */
export async function diagnoseScreenshot(endpoint, connect) {
  const report = {};
  const probe = async (timeoutMs, work) => {
    let connection;
    try {
      connection = await connect(endpoint, AbortSignal.timeout(timeoutMs));
      return await work(connection);
    } finally {
      if (connection) {
        connection.close();
        await connection.closed;
      }
    }
  };
  try {
    const { targetInfos } = await probe(3000, (connection) =>
      connection.call("Target.getTargets"),
    );
    report.targets = targetInfos
      .slice(0, 32)
      .map((target) => ({ id: target.targetId, type: target.type }));
    const target =
      targetInfos.find(
        (target) =>
          target.type === "page" &&
          target.url.startsWith("data:") &&
          target.url.includes("Managed"),
      ) ?? targetInfos.find((target) => target.type === "page");
    if (!target) return { ...report, error: "browser_page_unavailable" };
    report.selected = target.targetId;
    const capture = async (activate) => {
      try {
        return await probe(5000, async (connection) => {
          if (activate)
            await connection.call("Target.activateTarget", {
              targetId: target.targetId,
            });
          const { sessionId } = await connection.call("Target.attachToTarget", {
            targetId: target.targetId,
            flatten: true,
          });
          const { data } = await connection.call(
            "Page.captureScreenshot",
            {},
            sessionId,
          );
          return { ok: true, byteSize: Buffer.byteLength(data, "base64") };
        });
      } catch (error) {
        return { ok: false, error: code(error) };
      }
    };
    report.raw = await capture(false);
    if (!report.raw.ok) report.afterActivate = await capture(true);
  } catch (error) {
    report.error = code(error);
  }
  return report;
}
