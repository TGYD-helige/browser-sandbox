import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { sessionRequest } from "../../../dist/session.js";

const server = new McpServer({ name: "browser-session", version: "1.0.0" });
server.registerTool(
  "session",
  {
    description:
      "Manage this execution's browser session. Open a page before retain. Renew extends retention within the original browser deadline; release ends retention.",
    inputSchema: {
      action: z.enum(["status", "retain", "renew", "release", "stop"]),
      durationMs: z.number().int().min(1).max(1800000).optional(),
    },
  },
  async ({ action, durationMs }) => ({
    content: [
      {
        type: "text",
        text: JSON.stringify(
          await sessionRequest(process.env.BROWSER_WS_ENDPOINT, action, durationMs),
        ),
      },
    ],
  }),
);
await server.connect(new StdioServerTransport());
