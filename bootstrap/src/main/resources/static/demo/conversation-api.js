export function createConversationApi() {
  let token = "";
  let workspaceId = "";
  let controller = null;
  let generation = 0;

  function connect(nextToken, nextWorkspaceId) {
    controller?.abort();
    controller = new AbortController();
    token = nextToken;
    workspaceId = nextWorkspaceId;
    generation += 1;
    return generation;
  }

  function disconnect() {
    controller?.abort();
    controller = null;
    token = "";
    workspaceId = "";
    generation += 1;
  }

  async function request(path, options = {}) {
    if (!token) throw new Error("请先连接本地身份。");
    const headers = new Headers(options.headers || {});
    headers.set("Authorization", `Bearer ${token}`);
    if (options.body !== undefined && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
    const response = await fetch(`/api/v1/workspaces/${encodeURIComponent(workspaceId)}${path}`, {
      ...options, headers, signal: controller?.signal
    });
    const raw = await response.text();
    let data = null;
    if (raw) { try { data = JSON.parse(raw); } catch { data = { message: raw }; } }
    if (!response.ok) {
      throw new Error([response.status, data?.code || data?.errorCode, data?.message || data?.detail || response.statusText]
        .filter(Boolean).join(" · "));
    }
    return data;
  }

  return {
    connect, disconnect, request,
    key: () => crypto.randomUUID(),
    generation: () => generation,
    connected: () => Boolean(token),
    workspaceId: () => workspaceId
  };
}
