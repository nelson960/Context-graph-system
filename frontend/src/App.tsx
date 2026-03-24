import {
  Fragment,
  startTransition,
  useDeferredValue,
  useEffect,
  useMemo,
  useRef,
  useState,
  type FormEvent,
  type PointerEvent as ReactPointerEvent,
  type ReactNode,
} from "react";
import cytoscape, { type Core, type ElementDefinition, type StylesheetJson } from "cytoscape";

import {
  fetchEntity,
  fetchGraphQuery,
  searchEntities,
  submitChatQueryStream,
} from "./api";
import type {
  ChatQueryResponse,
  ChatStreamEvent,
  EntityDetailResponse,
  EntitySearchResult,
  GraphRequest,
  GraphResponse,
} from "./types";

const NODE_TYPE_CLASS: Record<string, string> = {
  SalesOrder: "commercial",
  SalesOrderItem: "commercial",
  ScheduleLine: "commercial",
  Delivery: "commercial",
  DeliveryItem: "commercial",
  BillingDocument: "commercial",
  BillingItem: "commercial",
  JournalEntry: "finance",
  Payment: "finance",
  Customer: "master",
  Address: "master",
  Product: "master",
  Plant: "master",
  StorageLocation: "master",
  CompanyCode: "finance",
  SalesArea: "master",
};

const graphStyle: StylesheetJson = [
  {
    selector: "node",
    style: {
      label: "data(label)",
      "font-size": "10px",
      "font-family": "\"Avenir Next\", \"Trebuchet MS\", sans-serif",
      color: "#1b2330",
      "text-wrap": "wrap",
      "text-max-width": "110px",
      "background-color": "#6b7280",
      width: 36,
      height: 36,
      "border-width": 2,
      "border-color": "#f8fafc",
      "text-valign": "bottom",
      "text-margin-y": 8,
    },
  },
  {
    selector: "node.commercial",
    style: {
      shape: "round-rectangle",
      "background-color": "#0f766e",
    },
  },
  {
    selector: "node.finance",
    style: {
      shape: "diamond",
      "background-color": "#b45309",
    },
  },
  {
    selector: "node.master",
    style: {
      shape: "ellipse",
      "background-color": "#2563eb",
    },
  },
  {
    selector: "node.highlighted",
    style: {
      "border-color": "#f97316",
      "border-width": 4,
      "font-weight": 700,
    },
  },
  {
    selector: "node.selected",
    style: {
      "border-color": "#0f172a",
      "border-width": 4,
    },
  },
  {
    selector: "edge",
    style: {
      width: 2,
      "curve-style": "bezier",
      "target-arrow-shape": "triangle",
      "arrow-scale": 0.9,
      "line-color": "#94a3b8",
      "target-arrow-color": "#94a3b8",
      "font-size": "8px",
      color: "#64748b",
      "text-background-color": "#ffffff",
      "text-background-opacity": 0.9,
      "text-background-padding": "2px",
    },
  },
  {
    selector: "edge.highlighted",
    style: {
      width: 4,
      "line-color": "#ea580c",
      "target-arrow-color": "#ea580c",
      label: "data(label)",
    },
  },
];

type Message = {
  id: string;
  role: "user" | "assistant";
  content: string;
  meta?: ChatQueryResponse;
};

type PopoverPosition = {
  x: number;
  y: number;
};

type MarkdownBlock =
  | { type: "text"; content: string }
  | { type: "code"; language: string | null; content: string };

export function App() {
  const [searchText, setSearchText] = useState("");
  const deferredSearchText = useDeferredValue(searchText);
  const [searchResults, setSearchResults] = useState<EntitySearchResult[]>([]);
  const [conversationId, setConversationId] = useState<string | null>(null);
  const [clusterMode, setClusterMode] = useState<"type" | null>(null);
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
  const [selectedNodeDetail, setSelectedNodeDetail] = useState<EntityDetailResponse | null>(null);
  const [graphData, setGraphData] = useState<GraphResponse | null>(null);
  const [graphRequest, setGraphRequest] = useState<GraphRequest | null>(null);
  const [graphError, setGraphError] = useState<string | null>(null);
  const [chatInput, setChatInput] = useState("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [chatBusy, setChatBusy] = useState(false);
  const [latestResponse, setLatestResponse] = useState<ChatQueryResponse | null>(null);
  const [isTracing, setIsTracing] = useState(false);
  const [popoverPosition, setPopoverPosition] = useState<PopoverPosition>({ x: 16, y: 16 });
  const [isPopoverDragging, setIsPopoverDragging] = useState(false);
  const graphFrameRef = useRef<HTMLDivElement | null>(null);
  const graphRef = useRef<HTMLDivElement | null>(null);
  const chatLogRef = useRef<HTMLDivElement | null>(null);
  const popoverRef = useRef<HTMLElement | null>(null);
  const dragStateRef = useRef<{
    offsetX: number;
    offsetY: number;
  } | null>(null);
  const cyRef = useRef<Core | null>(null);

  function scrollChatLogToBottom() {
    if (!chatLogRef.current) {
      return;
    }
    requestAnimationFrame(() => {
      if (!chatLogRef.current) {
        return;
      }
      chatLogRef.current.scrollTop = chatLogRef.current.scrollHeight;
    });
  }

  function clampPopoverPosition(position: PopoverPosition): PopoverPosition {
    const frame = graphFrameRef.current;
    if (!frame) {
      return position;
    }
    const margin = 16;
    const popoverWidth = popoverRef.current?.offsetWidth ?? Math.min(360, frame.clientWidth - margin * 2);
    const popoverHeight = popoverRef.current?.offsetHeight ?? Math.min(520, frame.clientHeight - margin * 2);
    const maxX = Math.max(margin, frame.clientWidth - popoverWidth - margin);
    const maxY = Math.max(margin, frame.clientHeight - popoverHeight - margin);
    return {
      x: Math.min(Math.max(margin, position.x), maxX),
      y: Math.min(Math.max(margin, position.y), maxY),
    };
  }

  function resetPopoverPosition() {
    const frame = graphFrameRef.current;
    if (!frame) {
      return;
    }
    const margin = 16;
    const popoverWidth = popoverRef.current?.offsetWidth ?? Math.min(360, frame.clientWidth - margin * 2);
    setPopoverPosition(
      clampPopoverPosition({
        x: frame.clientWidth - popoverWidth - margin,
        y: margin,
      }),
    );
  }

  function stopPopoverDrag() {
    dragStateRef.current = null;
    setIsPopoverDragging(false);
    window.removeEventListener("pointermove", handleWindowPointerMove);
    window.removeEventListener("pointerup", handleWindowPointerUp);
    window.removeEventListener("pointercancel", handleWindowPointerUp);
  }

  function handleWindowPointerMove(event: PointerEvent) {
    const frame = graphFrameRef.current;
    const dragState = dragStateRef.current;
    if (!frame || !dragState) {
      return;
    }
    const frameRect = frame.getBoundingClientRect();
    const nextPosition = {
      x: event.clientX - frameRect.left - dragState.offsetX,
      y: event.clientY - frameRect.top - dragState.offsetY,
    };
    setPopoverPosition(clampPopoverPosition(nextPosition));
  }

  function handleWindowPointerUp() {
    stopPopoverDrag();
  }

  useEffect(() => {
    if (!graphRef.current || cyRef.current) {
      return;
    }
    cyRef.current = cytoscape({
      container: graphRef.current,
      style: graphStyle,
      layout: { name: "grid" },
      elements: [],
      wheelSensitivity: 0.2,
    });
    cyRef.current.on("tap", "node", (event) => {
      const nodeId = event.target.id();
      void handleSelectNode(nodeId);
    });
    return () => {
      cyRef.current?.destroy();
      cyRef.current = null;
    };
  }, []);

  useEffect(() => {
    if (!graphRef.current || !cyRef.current) {
      return;
    }
    const observer = new ResizeObserver(() => {
      const cy = cyRef.current;
      if (!cy) {
        return;
      }
      cy.resize();
      if (cy.elements().length > 0) {
        cy.fit(cy.elements(), 56);
      }
      if (selectedNodeDetail) {
        setPopoverPosition((current) => clampPopoverPosition(current));
      }
    });
    observer.observe(graphRef.current);
    if (graphFrameRef.current) {
      observer.observe(graphFrameRef.current);
    }
    return () => {
      observer.disconnect();
    };
  }, [selectedNodeDetail]);

  useEffect(() => {
    const query = deferredSearchText.trim();
    if (query.length < 2) {
      setSearchResults([]);
      return;
    }
    let cancelled = false;
    void searchEntities(query)
      .then((results) => {
        if (!cancelled) {
          setSearchResults(results);
        }
      })
      .catch((error: Error) => {
        if (!cancelled) {
          setGraphError(error.message);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [deferredSearchText]);

  useEffect(() => {
    if (!cyRef.current) {
      return;
    }
    const cy = cyRef.current;
    const elements = graphElements(graphData, selectedNodeId, latestResponse);
    cy.elements().remove();
    cy.add(elements);
    const layoutName = elements.length > 36 ? "cose" : "breadthfirst";
    const layout = cy.layout({
      name: layoutName,
      animate: false,
      spacingFactor: 1.15,
      directed: true,
      padding: 56,
      fit: true,
    });
    layout.on("layoutstop", () => {
      cy.resize();
      if (cy.elements().length > 0) {
        cy.fit(cy.elements(), 56);
      }
    });
    layout.run();
  }, [graphData, latestResponse, selectedNodeId]);

  const selectedMetadata = useMemo(() => {
    if (!selectedNodeDetail) {
      return [];
    }
    return Object.entries(selectedNodeDetail.node.metadata).slice(0, 12);
  }, [selectedNodeDetail]);

  useEffect(() => {
    if (!selectedNodeDetail) {
      return;
    }
    resetPopoverPosition();
  }, [selectedNodeDetail?.node.id]);

  useEffect(() => {
    scrollChatLogToBottom();
  }, [messages]);

  useEffect(() => () => {
    stopPopoverDrag();
  }, []);

  async function handleSelectNode(nodeId: string) {
    setSelectedNodeId(nodeId);
    const detail = await fetchEntity(nodeId);
    startTransition(() => {
      setSelectedNodeDetail(detail);
    });
  }

  async function loadGraph(request: GraphRequest, nextClusterMode: "type" | null = clusterMode) {
    const graph = await fetchGraphQuery({
      ...request,
      cluster_mode: nextClusterMode,
    });
    startTransition(() => {
      setGraphRequest(request);
      setGraphData(graph);
      setGraphError(null);
    });
    return graph;
  }

  async function handleSearchPick(result: EntitySearchResult) {
    setSearchText(result.display_label);
    setSearchResults([]);
    const request: GraphRequest = {
      mode: "subgraph",
      node_ids: [result.node_id],
      depth: 1,
      include_hidden: false,
    };
    const [detail, graph] = await Promise.all([
      fetchEntity(result.node_id),
      loadGraph(request),
    ]);
    startTransition(() => {
      setSelectedNodeId(result.node_id);
      setSelectedNodeDetail(detail);
      setGraphData(graph);
      setLatestResponse(null);
    });
  }

  async function handleExpandSelected() {
    if (!selectedNodeId) {
      return;
    }
    await loadGraph({
      mode: "subgraph",
      node_ids: [selectedNodeId],
      depth: 2,
      include_hidden: true,
    });
  }

  async function handleTraceSelected() {
    if (!selectedNodeId) {
      return;
    }
    setIsTracing(true);
    try {
      await loadGraph({
        mode: "path",
        node_ids: [selectedNodeId],
        depth: 6,
        direction: "both",
      });
    } finally {
      setIsTracing(false);
    }
  }

  async function handleToggleClusterMode() {
    const nextMode = clusterMode ? null : "type";
    setClusterMode(nextMode);
    if (!graphRequest) {
      return;
    }
    await loadGraph(graphRequest, nextMode);
  }

  function handleCloseNodeDetail() {
    stopPopoverDrag();
    setSelectedNodeId(null);
    setSelectedNodeDetail(null);
  }

  function handlePopoverPointerDown(event: ReactPointerEvent<HTMLDivElement>) {
    if (!graphFrameRef.current || !popoverRef.current) {
      return;
    }
    event.preventDefault();
    const popoverRect = popoverRef.current.getBoundingClientRect();
    dragStateRef.current = {
      offsetX: event.clientX - popoverRect.left,
      offsetY: event.clientY - popoverRect.top,
    };
    setIsPopoverDragging(true);
    window.addEventListener("pointermove", handleWindowPointerMove);
    window.addEventListener("pointerup", handleWindowPointerUp);
    window.addEventListener("pointercancel", handleWindowPointerUp);
  }

  async function handleSubmitChat(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const message = chatInput.trim();
    if (!message) {
      return;
    }
    const userMessage: Message = {
      id: crypto.randomUUID(),
      role: "user",
      content: message,
    };
    const assistantMessageId = crypto.randomUUID();
    setMessages((current) => [
      ...current,
      userMessage,
      {
        id: assistantMessageId,
        role: "assistant",
        content: "",
      },
    ]);
    setChatBusy(true);
    setChatInput("");
    try {
      await submitChatQueryStream(
        {
          message,
          selectedNodeIds: selectedNodeId ? [selectedNodeId] : [],
          conversationId,
          clusterMode,
        },
        async (streamEvent: ChatStreamEvent) => {
          if (streamEvent.type === "conversation") {
            setConversationId(streamEvent.conversation_id);
            return;
          }
          if (streamEvent.type === "status") {
            setMessages((current) =>
              current.map((item) =>
                item.id === assistantMessageId && !item.content
                  ? { ...item, content: streamEvent.message }
                  : item,
              ),
            );
            return;
          }
          if (streamEvent.type === "answer_delta") {
            setMessages((current) =>
              current.map((item) =>
                item.id === assistantMessageId
                  ? {
                      ...item,
                      content:
                        item.content === "Planning query" || item.content === "Composing answer"
                          ? streamEvent.delta
                          : item.content + streamEvent.delta,
                    }
                  : item,
              ),
            );
            return;
          }
          if (streamEvent.type === "error") {
            setMessages((current) =>
              current.map((item) =>
                item.id === assistantMessageId
                  ? { ...item, content: streamEvent.error }
                  : item,
              ),
            );
            return;
          }
          if (streamEvent.type !== "final") {
            return;
          }
          const response = streamEvent.data;
          startTransition(() => {
            setConversationId(response.conversation_id ?? conversationId);
            setLatestResponse(response);
            setMessages((current) =>
              current.map((item) =>
                item.id === assistantMessageId
                  ? {
                      ...item,
                      content:
                        response.answer ??
                        response.error ??
                        (
                          item.content &&
                          item.content !== "Planning query" &&
                          item.content !== "Composing answer"
                            ? item.content
                            : "No response returned."
                        ),
                      meta: response,
                    }
                  : item,
              ),
            );
          });
          if (response.graph) {
            startTransition(() => {
              setGraphData(response.graph);
              setGraphRequest(response.graph_request);
              setSelectedNodeId(response.graph_center_node_id);
              setGraphError(null);
            });
          }
          if (response.graph_center_node_id) {
            const detail = await fetchEntity(response.graph_center_node_id);
            startTransition(() => {
              setSelectedNodeDetail(detail);
            });
          }
        },
      );
    } catch (error) {
      const messageText =
        error instanceof Error ? error.message : "Chat request failed";
      setMessages((current) => [
        ...current.map((item) =>
          item.id === assistantMessageId
            ? { ...item, content: messageText }
            : item,
        ),
      ]);
    } finally {
      setChatBusy(false);
    }
  }

  return (
    <div className="shell">
      <header className="masthead">
        <div>
          <p className="eyebrow">SAP Order-to-Cash</p>
          <h1>Context Graph</h1>
        </div>
        <div className="status-pill">
          <span className="status-dot" />
          Grounded analytics over normalized document flow
        </div>
      </header>

      <main className="workspace">
        <section className="graph-pane">
          <div className="pane-toolbar">
            <div className="search-box">
              <label htmlFor="entity-search">Find an entity</label>
              <input
                id="entity-search"
                value={searchText}
                onChange={(event) => setSearchText(event.target.value)}
                placeholder="Billing document, customer, product, sales order..."
              />
              {searchResults.length > 0 ? (
                <div className="search-results">
                  {searchResults.map((result) => (
                    <button
                      key={result.node_id}
                      className="search-result"
                      onClick={() => void handleSearchPick(result)}
                    >
                      <span>{result.display_label}</span>
                      <small>
                        {result.node_type} · {result.business_key}
                      </small>
                    </button>
                  ))}
                </div>
              ) : null}
            </div>

            <div className="graph-actions">
              <button onClick={() => void handleToggleClusterMode()}>
                {clusterMode ? "Uncluster" : "Cluster Types"}
              </button>
              <button onClick={() => void handleExpandSelected()} disabled={!selectedNodeId}>
                Expand Selected
              </button>
              <button onClick={() => void handleTraceSelected()} disabled={!selectedNodeId || isTracing}>
                {isTracing ? "Tracing…" : "Trace Flow"}
              </button>
            </div>
          </div>

          <div ref={graphFrameRef} className="graph-frame">
            <div ref={graphRef} className="graph-canvas" />
            {!graphData ? (
              <div className="graph-empty">
                Search for a document, customer, or product to start exploring the graph.
              </div>
            ) : null}
            {selectedNodeDetail ? (
              <aside
                ref={popoverRef}
                className={`node-popover ${isPopoverDragging ? "dragging" : ""}`}
                style={{ left: `${popoverPosition.x}px`, top: `${popoverPosition.y}px` }}
              >
                <div className="node-popover-header" onPointerDown={handlePopoverPointerDown}>
                  <div>
                    <div className="node-popover-type">{selectedNodeDetail.node.type}</div>
                    <h3>{selectedNodeDetail.node.display_label}</h3>
                    <p>{selectedNodeDetail.node.business_key}</p>
                  </div>
                  <button
                    type="button"
                    className="node-popover-close"
                    onClick={handleCloseNodeDetail}
                    onPointerDown={(event) => event.stopPropagation()}
                    aria-label="Close node detail"
                    title="Close node detail"
                  >
                    ×
                  </button>
                </div>

                <div className="node-popover-stats">
                  <div>
                    <span>Status</span>
                    <strong>{selectedNodeDetail.node.status ?? "NA"}</strong>
                  </div>
                  <div>
                    <span>Date</span>
                    <strong>{selectedNodeDetail.node.document_date ?? "NA"}</strong>
                  </div>
                  <div>
                    <span>Inbound</span>
                    <strong>{selectedNodeDetail.node.inbound_edge_count ?? 0}</strong>
                  </div>
                  <div>
                    <span>Outbound</span>
                    <strong>{selectedNodeDetail.node.outbound_edge_count ?? 0}</strong>
                  </div>
                </div>

                <div className="node-popover-meta">
                  {selectedMetadata.map(([key, value]) => (
                    <div key={key} className="node-popover-row">
                      <span>{key}</span>
                      <code>{String(value)}</code>
                    </div>
                  ))}
                </div>
              </aside>
            ) : null}
          </div>

          {graphError ? <div className="error-banner">{graphError}</div> : null}
        </section>

        <section className="info-pane">
          <div className="panel chat-panel">
            <div ref={chatLogRef} className="chat-log">
              {messages.length === 0 ? (
                <div className="empty-card">
                  Ask about billed products, incomplete flows, customer activity, or trace a billing document.
                </div>
              ) : (
                messages.map((message) => (
                  <article key={message.id} className={`message ${message.role}`}>
                    <div className="message-content">{renderMarkdown(message.content)}</div>
                    {message.meta?.sql ? (
                      <details className="message-meta">
                        <summary>SQL and provenance</summary>
                        <pre>{message.meta.sql}</pre>
                        {message.meta.provenance_note ? (
                          <p>{message.meta.provenance_note}</p>
                        ) : null}
                      </details>
                    ) : null}
                  </article>
                ))
              )}
            </div>
            <form className="chat-form" onSubmit={handleSubmitChat}>
              <div className="query-bar">
                <input
                  className="query-input"
                  value={chatInput}
                  onChange={(event) => setChatInput(event.target.value)}
                  placeholder="Ask about billed products, broken flows, or trace a document"
                />
                <button
                  type="submit"
                  className="query-send"
                  disabled={chatBusy}
                  aria-label={chatBusy ? "Running query" : "Send query"}
                  title={chatBusy ? "Running query" : "Send query"}
                >
                  <span aria-hidden="true">{chatBusy ? "…" : "↑"}</span>
                </button>
              </div>
            </form>
          </div>
        </section>
      </main>
    </div>
  );
}

function graphElements(
  graphData: GraphResponse | null,
  selectedNodeId: string | null,
  latestResponse: ChatQueryResponse | null,
): ElementDefinition[] {
  if (!graphData) {
    return [];
  }
  const highlightedNodes = new Set(latestResponse?.highlighted_node_ids ?? []);
  const highlightedEdges = new Set(latestResponse?.highlighted_edge_ids ?? []);

  const nodes = graphData.nodes.map((node) => ({
    data: {
      id: node.id,
      label: node.display_label,
      subtitle: node.subtitle,
    },
    classes: [
      NODE_TYPE_CLASS[node.type] ?? "master",
      highlightedNodes.has(node.id) ? "highlighted" : "",
      selectedNodeId === node.id ? "selected" : "",
    ]
      .filter(Boolean)
      .join(" "),
  }));

  const edges = graphData.edges.map((edge) => ({
    data: {
      id: edge.id,
      source: edge.source,
      target: edge.target,
      label: edge.type,
    },
    classes: highlightedEdges.has(edge.id) ? "highlighted" : "",
  }));
  return [...nodes, ...edges];
}

function renderMarkdown(content: string): ReactNode {
  const blocks = parseMarkdownBlocks(content);
  return blocks.map((block, blockIndex) => {
    if (block.type === "code") {
      return (
        <pre key={`code-${blockIndex}`} className="markdown-code-block">
          {block.language ? <span className="markdown-code-language">{block.language}</span> : null}
          <code>{block.content}</code>
        </pre>
      );
    }
    return renderMarkdownTextBlock(block.content, `text-${blockIndex}`);
  });
}

function parseMarkdownBlocks(content: string): MarkdownBlock[] {
  const blocks: MarkdownBlock[] = [];
  const normalized = content.replace(/\r\n/g, "\n");
  const codeFencePattern = /```([a-zA-Z0-9_-]+)?\n([\s\S]*?)```/g;
  let lastIndex = 0;
  let match = codeFencePattern.exec(normalized);

  while (match) {
    if (match.index > lastIndex) {
      blocks.push({
        type: "text",
        content: normalized.slice(lastIndex, match.index),
      });
    }
    blocks.push({
      type: "code",
      language: match[1] ?? null,
      content: match[2].replace(/\n$/, ""),
    });
    lastIndex = match.index + match[0].length;
    match = codeFencePattern.exec(normalized);
  }

  if (lastIndex < normalized.length) {
    blocks.push({
      type: "text",
      content: normalized.slice(lastIndex),
    });
  }

  return blocks.length > 0 ? blocks : [{ type: "text", content: normalized }];
}

function renderMarkdownTextBlock(content: string, keyPrefix: string): ReactNode {
  const lines = content.split("\n");
  const blocks: ReactNode[] = [];
  let index = 0;

  while (index < lines.length) {
    const line = lines[index].trim();
    if (!line) {
      index += 1;
      continue;
    }

    const headingMatch = /^(#{1,3})\s+(.*)$/.exec(line);
    if (headingMatch) {
      const level = headingMatch[1].length;
      const children = renderInlineMarkdown(headingMatch[2], `${keyPrefix}-heading-${index}`);
      if (level === 1) {
        blocks.push(<h1 key={`${keyPrefix}-h1-${index}`}>{children}</h1>);
      } else if (level === 2) {
        blocks.push(<h2 key={`${keyPrefix}-h2-${index}`}>{children}</h2>);
      } else {
        blocks.push(<h3 key={`${keyPrefix}-h3-${index}`}>{children}</h3>);
      }
      index += 1;
      continue;
    }

    if (/^[-*]\s+/.test(line)) {
      const items: ReactNode[] = [];
      let itemIndex = index;
      while (itemIndex < lines.length) {
        const itemLine = lines[itemIndex].trim();
        const itemMatch = /^[-*]\s+(.*)$/.exec(itemLine);
        if (!itemMatch) {
          break;
        }
        items.push(
          <li key={`${keyPrefix}-ul-${itemIndex}`}>
            {renderInlineMarkdown(itemMatch[1], `${keyPrefix}-ul-item-${itemIndex}`)}
          </li>,
        );
        itemIndex += 1;
      }
      blocks.push(
        <ul key={`${keyPrefix}-ul-group-${index}`} className="markdown-list">
          {items}
        </ul>,
      );
      index = itemIndex;
      continue;
    }

    if (/^\d+\.\s+/.test(line)) {
      const items: ReactNode[] = [];
      let itemIndex = index;
      while (itemIndex < lines.length) {
        const itemLine = lines[itemIndex].trim();
        const itemMatch = /^\d+\.\s+(.*)$/.exec(itemLine);
        if (!itemMatch) {
          break;
        }
        items.push(
          <li key={`${keyPrefix}-ol-${itemIndex}`}>
            {renderInlineMarkdown(itemMatch[1], `${keyPrefix}-ol-item-${itemIndex}`)}
          </li>,
        );
        itemIndex += 1;
      }
      blocks.push(
        <ol key={`${keyPrefix}-ol-group-${index}`} className="markdown-list">
          {items}
        </ol>,
      );
      index = itemIndex;
      continue;
    }

    if (/^>\s+/.test(line)) {
      const quoteLines: string[] = [];
      let quoteIndex = index;
      while (quoteIndex < lines.length) {
        const quoteLine = lines[quoteIndex].trim();
        const quoteMatch = /^>\s+(.*)$/.exec(quoteLine);
        if (!quoteMatch) {
          break;
        }
        quoteLines.push(quoteMatch[1]);
        quoteIndex += 1;
      }
      blocks.push(
        <blockquote key={`${keyPrefix}-blockquote-${index}`}>
          {renderInlineMarkdown(quoteLines.join(" "), `${keyPrefix}-blockquote-inline-${index}`)}
        </blockquote>,
      );
      index = quoteIndex;
      continue;
    }

    const paragraphLines: string[] = [line];
    let paragraphIndex = index + 1;
    while (paragraphIndex < lines.length) {
      const paragraphLine = lines[paragraphIndex].trim();
      if (
        !paragraphLine ||
        /^(#{1,3})\s+/.test(paragraphLine) ||
        /^[-*]\s+/.test(paragraphLine) ||
        /^\d+\.\s+/.test(paragraphLine) ||
        /^>\s+/.test(paragraphLine)
      ) {
        break;
      }
      paragraphLines.push(paragraphLine);
      paragraphIndex += 1;
    }
    blocks.push(
      <p key={`${keyPrefix}-p-${index}`}>
        {renderInlineMarkdown(paragraphLines.join(" "), `${keyPrefix}-p-inline-${index}`)}
      </p>,
    );
    index = paragraphIndex;
  }

  return <Fragment key={keyPrefix}>{blocks}</Fragment>;
}

function renderInlineMarkdown(content: string, keyPrefix: string): ReactNode[] {
  const nodes: ReactNode[] = [];
  const pattern = /(\*\*[^*]+\*\*|`[^`]+`|\[[^\]]+\]\([^)]+\))/g;
  let lastIndex = 0;
  let match = pattern.exec(content);

  while (match) {
    if (match.index > lastIndex) {
      nodes.push(content.slice(lastIndex, match.index));
    }
    const token = match[0];
    if (token.startsWith("**") && token.endsWith("**")) {
      nodes.push(
        <strong key={`${keyPrefix}-${match.index}`}>
          {token.slice(2, -2)}
        </strong>,
      );
    } else if (token.startsWith("`") && token.endsWith("`")) {
      nodes.push(
        <code key={`${keyPrefix}-${match.index}`} className="markdown-inline-code">
          {token.slice(1, -1)}
        </code>,
      );
    } else {
      const linkMatch = /^\[([^\]]+)\]\(([^)]+)\)$/.exec(token);
      if (linkMatch) {
        nodes.push(
          <a
            key={`${keyPrefix}-${match.index}`}
            href={linkMatch[2]}
            target="_blank"
            rel="noreferrer"
          >
            {linkMatch[1]}
          </a>,
        );
      } else {
        nodes.push(token);
      }
    }
    lastIndex = match.index + token.length;
    match = pattern.exec(content);
  }

  if (lastIndex < content.length) {
    nodes.push(content.slice(lastIndex));
  }
  return nodes;
}
