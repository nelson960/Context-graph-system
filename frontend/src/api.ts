import type {
  ChatQueryRequest,
  ChatQueryResponse,
  ChatStreamEvent,
  EntityDetailResponse,
  EntitySearchResult,
  GraphResponse,
} from "./types";

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "";

async function apiRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(init?.headers ?? {}),
    },
    ...init,
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  return response.json() as Promise<T>;
}

export function searchEntities(query: string): Promise<EntitySearchResult[]> {
  return apiRequest(`/api/entities/search?q=${encodeURIComponent(query)}`);
}

export function fetchEntity(nodeId: string): Promise<EntityDetailResponse> {
  return apiRequest(`/api/entities/${encodeURIComponent(nodeId)}`);
}

export function fetchSubgraph(
  nodeId: string,
  depth: number,
  includeHidden = false,
  clusterMode?: "type" | null,
): Promise<GraphResponse> {
  return apiRequest(
    `/api/graph/subgraph?node_id=${encodeURIComponent(nodeId)}&depth=${depth}&include_hidden=${includeHidden}${
      clusterMode ? `&cluster_mode=${encodeURIComponent(clusterMode)}` : ""
    }`,
  );
}

export function fetchPath(
  nodeId: string,
  direction: "upstream" | "downstream" | "both",
  depth: number,
  clusterMode?: "type" | null,
): Promise<GraphResponse> {
  return apiRequest(
    `/api/graph/path?node_id=${encodeURIComponent(nodeId)}&direction=${direction}&depth=${depth}${
      clusterMode ? `&cluster_mode=${encodeURIComponent(clusterMode)}` : ""
    }`,
  );
}

export function submitChatQuery(
  payload: ChatQueryRequest,
): Promise<ChatQueryResponse> {
  return apiRequest("/api/chat/query", {
    method: "POST",
    body: JSON.stringify(payload),
  });
}

export async function submitChatQueryStream(
  payload: ChatQueryRequest,
  onEvent: (event: ChatStreamEvent) => void | Promise<void>,
): Promise<void> {
  const response = await fetch(`${API_BASE_URL}/api/chat/query/stream`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const text = await response.text();
    throw new Error(text || `HTTP ${response.status}`);
  }
  if (!response.body) {
    throw new Error("Streaming response body was not available");
  }
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) {
      break;
    }
    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split("\n");
    buffer = lines.pop() ?? "";
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) {
        continue;
      }
      await onEvent(JSON.parse(trimmed) as ChatStreamEvent);
    }
  }
  const tail = buffer.trim();
  if (tail) {
    await onEvent(JSON.parse(tail) as ChatStreamEvent);
  }
}
