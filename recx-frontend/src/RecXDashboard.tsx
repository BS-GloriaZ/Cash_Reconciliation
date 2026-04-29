import React, { useEffect, useMemo, useState } from "react";
import {
  Upload,
  Filter,
  Search,
  AlertTriangle,
  CheckCircle2,
  Wallet,
  Building2,
  CalendarDays,
  BarChart3,
  MessageSquare,
  Save,
  LayoutDashboard,
  Table2,
} from "lucide-react";
import {
  ResponsiveContainer,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
} from "recharts";
import * as XLSX from "xlsx";

type WorkbookSheets = Record<string, any[]>;
type CommentsMap = Record<string, string>;
type DraftsMap = Record<string, string>;

type ManifestPayload = {
  latest_file: string;
};

const COLORS = ["#2563eb", "#16a34a", "#f59e0b", "#dc2626", "#7c3aed", "#0891b2"];
const OUTPUTS_MANIFEST_URL = "/outputs/manifest.json";

function formatNumber(value: any) {
  const n = Number(value ?? 0);
  if (Number.isNaN(n)) return "-";
  return n.toLocaleString(undefined, { maximumFractionDigits: 2, minimumFractionDigits: 2 });
}

function toDateString(value: any) {
  if (value === null || value === undefined || value === "") return "";

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) return trimmed;
    const parsed = new Date(trimmed);
    if (!Number.isNaN(parsed.getTime())) return parsed.toISOString().slice(0, 10);
    return trimmed;
  }

  if (typeof value === "number") {
    if (value > 20000 && value < 100000) {
      const excelEpoch = new Date(Date.UTC(1899, 11, 30));
      const ms = value * 86400000;
      const d = new Date(excelEpoch.getTime() + ms);
      return d.toISOString().slice(0, 10);
    }
    return String(value);
  }

  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10);
  }

  return String(value);
}

function normalizeRows(rows: any[]) {
  return rows.map((row, idx) => {
    const out: Record<string, any> = { __rowId: idx };
    Object.entries(row || {}).forEach(([k, v]) => {
      out[String(k).trim()] = v;
    });
    return out;
  });
}

function parseWorkbook(file: File): Promise<WorkbookSheets> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const data = e.target?.result;
        const wb = XLSX.read(data, { type: "array" });
        const sheets: WorkbookSheets = {};
        wb.SheetNames.forEach((name) => {
          const ws = wb.Sheets[name];
          sheets[name] = normalizeRows(
            XLSX.utils.sheet_to_json(ws, {
              defval: "",
              raw: false,
            })
          );
        });
        resolve(sheets);
      } catch (err) {
        reject(err);
      }
    };
    reader.onerror = reject;
    reader.readAsArrayBuffer(file);
  });
}

async function parseWorkbookFromUrl(url: string): Promise<WorkbookSheets> {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Failed to load workbook from ${url}`);
  const buffer = await response.arrayBuffer();
  const wb = XLSX.read(buffer, { type: "array" });
  const sheets: WorkbookSheets = {};
  wb.SheetNames.forEach((name) => {
    const ws = wb.Sheets[name];
    sheets[name] = normalizeRows(
      XLSX.utils.sheet_to_json(ws, {
        defval: "",
        raw: false,
      })
    );
  });
  return sheets;
}

async function getLatestWorkbookUrl(): Promise<string> {
  const response = await fetch(OUTPUTS_MANIFEST_URL);
  if (!response.ok) throw new Error("Failed to load outputs manifest");
  const manifest = (await response.json()) as ManifestPayload;
  if (!manifest.latest_file) throw new Error("Manifest does not contain latest_file");
  return `/outputs/${manifest.latest_file}`;
}

function buildBreakKey(row: any, recType: string) {
  const parts = [
    recType,
    row.Date || row["Target Date"] || "",
    row["Mapping Fund"] || row.Fund || "",
    row.Portfolio || "",
    row["Currency Code"] || "",
    row["Tradar_Account"] || row["Tradar Accounts Used"] || "",
    row["Source Account ID"] || row["Source SKAC"] || row["BNP Cash Balance USD"] || "",
  ];
  return parts.map((x) => String(x ?? "").trim()).join("|");
}

function cardStyle(): React.CSSProperties {
  return {
    background: "#ffffff",
    border: "1px solid #e2e8f0",
    borderRadius: 20,
    boxShadow: "0 1px 2px rgba(15, 23, 42, 0.06)",
  };
}

function sectionHeaderStyle(): React.CSSProperties {
  return {
    padding: "16px 20px",
    borderBottom: "1px solid #e2e8f0",
    fontWeight: 600,
    color: "#0f172a",
  };
}

function sectionBodyStyle(): React.CSSProperties {
  return { padding: 16 };
}

function tdStyle(): React.CSSProperties {
  return {
    padding: "10px 12px",
    verticalAlign: "top",
    borderTop: "1px solid #e2e8f0",
    color: "#1e293b",
    whiteSpace: "nowrap",
  };
}

function selectStyle(): React.CSSProperties {
  return {
    width: "100%",
    padding: "10px 12px",
    borderRadius: 12,
    border: "1px solid #cbd5e1",
    background: "#fff",
  };
}

function inputStyle(): React.CSSProperties {
  return {
    width: "100%",
    padding: "10px 12px",
    borderRadius: 12,
    border: "1px solid #cbd5e1",
    background: "#fff",
  };
}

function MetricCard({ title, value, icon: Icon, tone = "default" }: any) {
  const bgMap: Record<string, string> = {
    default: "#ffffff",
    success: "#f0fdf4",
    warning: "#fffbeb",
    danger: "#fef2f2",
    info: "#eff6ff",
  };

  return (
    <div style={{ ...cardStyle(), background: bgMap[tone] || bgMap.default, padding: 20 }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
        <div>
          <div style={{ fontSize: 14, color: "#64748b" }}>{title}</div>
          <div style={{ fontSize: 28, fontWeight: 600, color: "#0f172a", marginTop: 6 }}>{value}</div>
        </div>
        <div
          style={{
            height: 44,
            width: 44,
            borderRadius: 16,
            background: "rgba(255,255,255,0.9)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            border: "1px solid #e2e8f0",
          }}
        >
          <Icon size={20} color="#334155" />
        </div>
      </div>
    </div>
  );
}

function EmptyState({ title, subtitle }: any) {
  return (
    <div style={{ ...cardStyle(), borderStyle: "dashed", padding: 48, textAlign: "center" }}>
      <div
        style={{
          margin: "0 auto 16px auto",
          height: 64,
          width: 64,
          borderRadius: 24,
          background: "#f1f5f9",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        <BarChart3 size={32} color="#64748b" />
      </div>
      <div style={{ fontSize: 24, fontWeight: 600, color: "#0f172a" }}>{title}</div>
      <div style={{ color: "#64748b", marginTop: 8 }}>{subtitle}</div>
    </div>
  );
}

function StatusBadge({ value }: { value: string }) {
  const isBreak = String(value) === "Break";
  return (
    <span
      style={{
        display: "inline-block",
        padding: "4px 10px",
        borderRadius: 999,
        fontSize: 12,
        fontWeight: 600,
        background: isBreak ? "#fee2e2" : "#dcfce7",
        color: isBreak ? "#b91c1c" : "#15803d",
      }}
    >
      {value || ""}
    </span>
  );
}

function TypeBadge({ value }: { value: string }) {
  return (
    <span
      style={{
        display: "inline-block",
        padding: "4px 10px",
        borderRadius: 999,
        fontSize: 12,
        border: "1px solid #cbd5e1",
        background: "#f8fafc",
        color: "#334155",
      }}
    >
      {value}
    </span>
  );
}

function SimpleTable({ rows }: { rows: any[] }) {
  const columns = rows.length ? Object.keys(rows[0]).filter((k) => k !== "__rowId") : [];
  if (!rows.length) return <div style={{ color: "#64748b" }}>No rows.</div>;

  return (
    <div style={{ overflow: "auto", border: "1px solid #e2e8f0", borderRadius: 16 }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 14, background: "#fff" }}>
        <thead style={{ background: "#f8fafc" }}>
          <tr>
            {columns.map((col) => (
              <th key={col} style={{ textAlign: "left", padding: "10px 12px", fontWeight: 600, color: "#475569", whiteSpace: "nowrap", borderBottom: "1px solid #e2e8f0" }}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.__rowId}>
              {columns.map((col) => (
                <td key={col} style={{ padding: "10px 12px", whiteSpace: "nowrap", color: "#1e293b", borderTop: "1px solid #e2e8f0" }}>
                  {String(row[col] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function DetailRecTable({ rows, comments, drafts, onDraftChange, onSave }: any) {
  if (!rows.length) return <div style={{ color: "#64748b" }}>No rows for the current filter.</div>;

  return (
    <div style={{ overflow: "auto", border: "1px solid #e2e8f0", borderRadius: 16 }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 14, background: "#fff" }}>
        <thead style={{ background: "#f8fafc" }}>
          <tr>
            {["Date", "Type", "Fund", "Custody", "Currency", "Tradar", "Abs Variance", "Status", "Comment"].map((col) => (
              <th key={col} style={{ textAlign: "left", padding: "10px 12px", fontWeight: 600, color: "#475569", whiteSpace: "nowrap", borderBottom: "1px solid #e2e8f0" }}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row: any) => {
            const key = row.__breakKey;
            const saved = comments[key] || "";
            const draft = drafts[key] ?? saved;
            return (
              <tr key={key}>
                <td style={tdStyle()}>{row.__displayDate}</td>
                <td style={tdStyle()}><TypeBadge value={row.__recType} /></td>
                <td style={tdStyle()}>{row["Mapping Fund"] || row.Fund || ""}</td>
                <td style={tdStyle()}>{row.Custody || ""}</td>
                <td style={tdStyle()}>{row["Currency Code"] || ""}</td>
                <td style={tdStyle()}>{row["Tradar_Account"] || row["Tradar Accounts Used"] || ""}</td>
                <td style={tdStyle()}>{formatNumber(row["Abs Variance"] || 0)}</td>
                <td style={tdStyle()}><StatusBadge value={row.Status || ""} /></td>
                <td style={{ ...tdStyle(), minWidth: 340 }}>
                  <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                    <textarea
                      value={draft}
                      onChange={(e) => onDraftChange(key, e.target.value)}
                      placeholder="Add comment for this break..."
                      style={{ width: "100%", minHeight: 88, borderRadius: 12, border: "1px solid #cbd5e1", padding: 10, resize: "vertical", fontFamily: "inherit", fontSize: 14 }}
                    />
                    <div style={{ display: "flex", justifyContent: "space-between", gap: 8, alignItems: "center" }}>
                      <div style={{ fontSize: 12, color: "#64748b" }}>{saved ? `Saved comment: ${saved}` : "No saved comment yet"}</div>
                      <button
                        onClick={() => onSave(key)}
                        style={{ display: "inline-flex", alignItems: "center", gap: 6, padding: "8px 12px", borderRadius: 12, border: "1px solid #0f172a", background: "#0f172a", color: "#fff", cursor: "pointer" }}
                      >
                        <Save size={16} /> Save
                      </button>
                    </div>
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export default function RecXDashboard() {
  const [workbook, setWorkbook] = useState<WorkbookSheets | null>(null);
  const [fileName, setFileName] = useState("");
  const [error, setError] = useState("");
  const [loadedWorkbookUrl, setLoadedWorkbookUrl] = useState("");
  const [isLoadingLatest, setIsLoadingLatest] = useState(false);
  const [comments, setComments] = useState<CommentsMap>({});
  const [drafts, setDrafts] = useState<DraftsMap>({});
  const [currentPage, setCurrentPage] = useState<"dashboard" | "detail">("dashboard");

  const [detailDateFilter, setDetailDateFilter] = useState("all");
  const [detailFundFilter, setDetailFundFilter] = useState("all");
  const [detailTypeFilter, setDetailTypeFilter] = useState("all");
  const [detailStatusFilter, setDetailStatusFilter] = useState("Break");
  const [detailMateriality, setDetailMateriality] = useState("0");
  const [detailAbsVarianceMin, setDetailAbsVarianceMin] = useState("");
  const [detailSearch, setDetailSearch] = useState("");

  const recDetail = workbook?.["Rec_Detail"] || [];
  const hiRecDetail = workbook?.["HI_Rec_Detail"] || [];
  const bbusRecDetail = workbook?.["BBUS_BNP_Rec_Detail"] || [];
  const summary = workbook?.["Summary"] || [];
  const hiSummary = workbook?.["HI_Summary"] || [];
  const bbusSummary = workbook?.["BBUS_BNP_Adjustment"] || [];

  useEffect(() => {
    try {
      const saved = localStorage.getItem("recx-comments");
      if (saved) setComments(JSON.parse(saved));
    } catch {
      // ignore local storage errors
    }
  }, []);

  useEffect(() => {
    try {
      localStorage.setItem("recx-comments", JSON.stringify(comments));
    } catch {
      // ignore local storage errors
    }
  }, [comments]);

  const dashboardBreaks = useMemo(() => {
    const normal = recDetail
      .filter((r) => {
        const fund = String(r["Mapping Fund"] || r.Fund || "").trim().toUpperCase();
        const custody = String(r.Custody || "").trim().toUpperCase();
        if (fund === "BBUS" && custody === "BNP") return false;
        return true;
      })
      .map((r) => ({ ...r, __recType: "Normal", __displayDate: toDateString(r.Date) }));

    const hi = hiRecDetail.map((r) => ({ ...r, __recType: "High Interest", __displayDate: toDateString(r["Target Date"]) }));
    const bbus = bbusRecDetail.map((r) => ({ ...r, __recType: "BNP Special", __displayDate: toDateString(r.Date) }));

    return [...normal, ...hi, ...bbus].map((r, idx) => ({ ...r, __rowId: `${r.__recType}-${idx}`, __breakKey: buildBreakKey(r, r.__recType) }));
  }, [recDetail, hiRecDetail, bbusRecDetail]);

  const detailRows = useMemo(() => {
  const normal = recDetail
    .filter((r) => {
      const fund = String(r["Mapping Fund"] || r.Fund || "").trim().toUpperCase();
      const custody = String(r.Custody || "").trim().toUpperCase();

      // suppress normal BBUS BNP rows to avoid confusion with BNP Special rec
      if (fund === "BBUS" && custody === "BNP") {
        return false;
      }
      return true;
    })
    .map((r) => ({
      ...r,
      __recType: "Normal",
      __displayDate: toDateString(r.Date),
    }));

  const hi = hiRecDetail.map((r) => ({
    ...r,
    __recType: "High Interest",
    __displayDate: toDateString(r["Target Date"]),
  }));

  const bbus = bbusRecDetail.map((r) => ({
    ...r,
    __recType: "BNP Special",
    __displayDate: toDateString(r.Date),
  }));

  return [...normal, ...hi, ...bbus].map((r, idx) => ({
    ...r,
    __rowId: `${r.__recType}-${idx}`,
    __breakKey: buildBreakKey(r, r.__recType),
  }));
}, [recDetail, hiRecDetail, bbusRecDetail]);

  const detailDates = useMemo(() => ["all", ...Array.from(new Set(detailRows.map((r) => r.__displayDate).filter(Boolean))).sort().reverse()], [detailRows]);
  const detailFunds = useMemo(() => ["all", ...Array.from(new Set(detailRows.map((r) => String(r["Mapping Fund"] || r.Fund || "")).filter(Boolean))).sort()], [detailRows]);
  const detailTypes = ["all", "Normal", "High Interest", "BNP Special"];

  const filteredDetailRows = useMemo(() => {
    const materialityValue = Number(detailMateriality || 0);
    const absVarianceMinValue = Number(detailAbsVarianceMin || 0);
    const minVariance = Math.max(materialityValue, absVarianceMinValue);

    return detailRows.filter((r) => {
      const fund = String(r["Mapping Fund"] || r.Fund || "");
      const status = String(r.Status || "");
      const absVar = Number(r["Abs Variance"] || 0);
      const haystack = `${JSON.stringify(r).toLowerCase()} ${String(comments[r.__breakKey] || "").toLowerCase()}`;
      return (detailDateFilter === "all" || r.__displayDate === detailDateFilter)
        && (detailFundFilter === "all" || fund === detailFundFilter)
        && (detailTypeFilter === "all" || r.__recType === detailTypeFilter)
        && (detailStatusFilter === "all" || status === detailStatusFilter)
        && absVar >= minVariance
        && (!detailSearch || haystack.includes(detailSearch.toLowerCase()));
    });
  }, [detailRows, detailDateFilter, detailFundFilter, detailTypeFilter, detailStatusFilter, detailMateriality, detailAbsVarianceMin, detailSearch, comments]);

  const dashboardKpis = useMemo(() => {
    const breaks = dashboardBreaks.filter((r) => String(r.Status) === "Break");
    const matched = dashboardBreaks.filter((r) => String(r.Status) === "Matched");
    const variance = dashboardBreaks.reduce((sum, r) => sum + Number(r["Abs Variance"] || 0), 0);
    return { total: dashboardBreaks.length, breaks: breaks.length, matched: matched.length, variance };
  }, [dashboardBreaks]);

  const trendData = useMemo(() => {
    const map = new Map<string, number>();
    dashboardBreaks.forEach((r) => {
      map.set(r.__displayDate, (map.get(r.__displayDate) || 0) + Number(r["Abs Variance"] || 0));
    });
    return Array.from(map.entries()).sort((a, b) => a[0].localeCompare(b[0])).map(([date, variance]) => ({ date, variance: Number(variance.toFixed(2)) }));
  }, [dashboardBreaks]);

  const custodyData = useMemo(() => {
    const map = new Map<string, number>();
    dashboardBreaks.forEach((r) => {
      const key = String(r.Custody || "Unknown");
      map.set(key, (map.get(key) || 0) + Number(r["Abs Variance"] || 0));
    });
    return Array.from(map.entries()).map(([name, value]) => ({ name, value: Number(value.toFixed(2)) }));
  }, [dashboardBreaks]);

  const topBreaks = useMemo(() => {
    return [...dashboardBreaks].filter((r) => String(r.Status) === "Break").sort((a, b) => Number(b["Abs Variance"] || 0) - Number(a["Abs Variance"] || 0)).slice(0, 15);
  }, [dashboardBreaks]);

  async function onUpload(file?: File) {
    if (!file) return;
    try {
      setError("");
      const parsed = await parseWorkbook(file);
      setWorkbook(parsed);
      setFileName(file.name);
      setLoadedWorkbookUrl("");
    } catch (e: any) {
      setError(e?.message || "Failed to read workbook");
    }
  }

  async function loadLatestWorkbook() {
    try {
      setIsLoadingLatest(true);
      setError("");
      const workbookUrl = await getLatestWorkbookUrl();
      const parsed = await parseWorkbookFromUrl(workbookUrl);
      setWorkbook(parsed);
      setLoadedWorkbookUrl(workbookUrl);
      setFileName(workbookUrl.split("/").pop() || "");
    } catch (e: any) {
      setError(e?.message || "Failed to load latest workbook");
    } finally {
      setIsLoadingLatest(false);
    }
  }

  useEffect(() => {
    loadLatestWorkbook();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  function onDraftChange(key: string, value: string) {
    setDrafts((prev) => ({ ...prev, [key]: value }));
  }

  function onSave(key: string) {
    setComments((prev) => ({ ...prev, [key]: drafts[key] ?? prev[key] ?? "" }));
  }

  return (
    <div style={{ minHeight: "100vh", background: "#f1f5f9", padding: 24 }}>
      <div style={{ maxWidth: 1440, margin: "0 auto", display: "flex", flexDirection: "column", gap: 24 }}>
        <div style={{ display: "flex", flexWrap: "wrap", gap: 16, justifyContent: "space-between", alignItems: "center" }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <div style={{ height: 48, width: 48, borderRadius: 16, background: "#0f172a", display: "flex", alignItems: "center", justifyContent: "center" }}>
              <Wallet size={24} color="#fff" />
            </div>
            <div>
              <div style={{ fontSize: 36, fontWeight: 700, color: "#0f172a" }}>RecX</div>
              <div style={{ color: "#64748b" }}>Dashboard on page 1. Detailed reconciliation on page 2.</div>
            </div>
          </div>

          <div style={{ ...cardStyle(), padding: 16, minWidth: 420, flex: 1, maxWidth: 720 }}>
            <div style={{ display: "flex", flexWrap: "wrap", gap: 12, alignItems: "center" }}>
              <label style={{ display: "inline-flex", alignItems: "center", gap: 8, cursor: "pointer", padding: "10px 14px", borderRadius: 12, border: "1px solid #0f172a", background: "#0f172a", color: "#fff" }}>
                <Upload size={16} /> Upload workbook
                <input type="file" accept=".xlsx,.xls" style={{ display: "none" }} onChange={(e) => onUpload(e.target.files?.[0])} />
              </label>
              <button onClick={loadLatestWorkbook} disabled={isLoadingLatest} style={{ padding: "10px 14px", borderRadius: 12, border: "1px solid #cbd5e1", background: "#fff", cursor: "pointer" }}>
                {isLoadingLatest ? "Loading..." : "Load latest output"}
              </button>
            </div>
            <div style={{ display: "grid", gridTemplateColumns: "1fr auto", gap: 12, marginTop: 12, alignItems: "center" }}>
              <input readOnly value={loadedWorkbookUrl} placeholder="Auto-discovered from outputs manifest" style={inputStyle()} />
              {fileName ? <span style={{ display: "inline-block", padding: "6px 10px", borderRadius: 999, fontSize: 12, border: "1px solid #cbd5e1", background: "#f8fafc", color: "#334155" }}>{fileName}</span> : null}
            </div>
          </div>
        </div>

        <div style={{ display: "flex", gap: 12 }}>
          <button
            onClick={() => setCurrentPage("dashboard")}
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 8,
              padding: "10px 14px",
              borderRadius: 12,
              border: currentPage === "dashboard" ? "1px solid #0f172a" : "1px solid #cbd5e1",
              background: currentPage === "dashboard" ? "#0f172a" : "#fff",
              color: currentPage === "dashboard" ? "#fff" : "#0f172a",
              cursor: "pointer",
            }}
          >
            <LayoutDashboard size={16} /> Page 1 Dashboard
          </button>
          <button
            onClick={() => setCurrentPage("detail")}
            style={{
              display: "inline-flex",
              alignItems: "center",
              gap: 8,
              padding: "10px 14px",
              borderRadius: 12,
              border: currentPage === "detail" ? "1px solid #0f172a" : "1px solid #cbd5e1",
              background: currentPage === "detail" ? "#0f172a" : "#fff",
              color: currentPage === "detail" ? "#fff" : "#0f172a",
              cursor: "pointer",
            }}
          >
            <Table2 size={16} /> Page 2 Detailed Rec
          </button>
        </div>

        {error ? <div style={{ ...cardStyle(), background: "#fef2f2", borderColor: "#fecaca", padding: 16, color: "#b91c1c" }}>{error}</div> : null}

        {!workbook ? (
          <EmptyState title="Upload a reconciliation workbook" subtitle="Load the latest reconciliation workbook automatically from the outputs manifest, or upload any Excel output manually." />
        ) : currentPage === "dashboard" ? (
          <>
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 16 }}>
              <MetricCard title="Total Rows" value={dashboardKpis.total.toLocaleString()} icon={BarChart3} tone="info" />
              <MetricCard title="Matched" value={dashboardKpis.matched.toLocaleString()} icon={CheckCircle2} tone="success" />
              <MetricCard title="Breaks" value={dashboardKpis.breaks.toLocaleString()} icon={AlertTriangle} tone="warning" />
              <MetricCard title="Abs Variance" value={formatNumber(dashboardKpis.variance)} icon={Building2} tone="danger" />
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 2fr) minmax(320px, 1fr)", gap: 24 }}>
              <div style={cardStyle()}>
                <div style={sectionHeaderStyle()}><div style={{ display: "flex", alignItems: "center", gap: 8 }}><CalendarDays size={18} /> Variance trend</div></div>
                <div style={{ ...sectionBodyStyle(), height: 320 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={trendData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="date" />
                      <YAxis />
                      <Tooltip />
                      <Line type="monotone" dataKey="variance" stroke="#0f172a" strokeWidth={2} dot={false} />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </div>
              <div style={cardStyle()}>
                <div style={sectionHeaderStyle()}>Custody split</div>
                <div style={{ ...sectionBodyStyle(), height: 320 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Pie data={custodyData} dataKey="value" nameKey="name" outerRadius={100} label>
                        {custodyData.map((entry, index) => <Cell key={entry.name} fill={COLORS[index % COLORS.length]} />)}
                      </Pie>
                      <Tooltip formatter={(v: any) => formatNumber(v)} />
                    </PieChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(400px, 1fr))", gap: 24 }}>
              <div style={cardStyle()}>
                <div style={sectionHeaderStyle()}>Top breaks</div>
                <div style={sectionBodyStyle()}>{topBreaks.length ? <SimpleTable rows={topBreaks} /> : <div style={{ color: "#64748b" }}>No break rows.</div>}</div>
              </div>
              <div style={cardStyle()}>
                <div style={sectionHeaderStyle()}>Variance by custody</div>
                <div style={{ ...sectionBodyStyle(), height: 360 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={custodyData}>
                      <CartesianGrid strokeDasharray="3 3" />
                      <XAxis dataKey="name" />
                      <YAxis />
                      <Tooltip formatter={(v: any) => formatNumber(v)} />
                      <Bar dataKey="value" fill="#0f172a" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>

            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: 24 }}>
              <div style={cardStyle()}><div style={sectionHeaderStyle()}>Summary</div><div style={sectionBodyStyle()}><SimpleTable rows={summary} /></div></div>
              <div style={cardStyle()}><div style={sectionHeaderStyle()}>HI_Summary</div><div style={sectionBodyStyle()}><SimpleTable rows={hiSummary} /></div></div>
              <div style={cardStyle()}><div style={sectionHeaderStyle()}>BBUS_BNP_Adjustment</div><div style={sectionBodyStyle()}><SimpleTable rows={bbusSummary} /></div></div>
            </div>
          </>
        ) : (
          <>
            <div style={cardStyle()}>
              <div style={sectionHeaderStyle()}><div style={{ display: "flex", alignItems: "center", gap: 8 }}><Filter size={18} /> Detailed Rec Filters</div></div>
              <div style={{ ...sectionBodyStyle(), display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: 16 }}>
                <select value={detailDateFilter} onChange={(e) => setDetailDateFilter(e.target.value)} style={selectStyle()}>
                  {detailDates.map((d) => <option key={d} value={d}>{d === "all" ? "All dates" : d}</option>)}
                </select>
                <select value={detailFundFilter} onChange={(e) => setDetailFundFilter(e.target.value)} style={selectStyle()}>
                  {detailFunds.map((f) => <option key={f} value={f}>{f === "all" ? "All funds" : f}</option>)}
                </select>
                <select value={detailTypeFilter} onChange={(e) => setDetailTypeFilter(e.target.value)} style={selectStyle()}>
                  {detailTypes.map((t) => <option key={t} value={t}>{t === "all" ? "All types" : t}</option>)}
                </select>
                <select value={detailStatusFilter} onChange={(e) => setDetailStatusFilter(e.target.value)} style={selectStyle()}>
                  <option value="all">All status</option>
                  <option value="Break">Break</option>
                  <option value="Matched">Matched</option>
                </select>
                <input value={detailMateriality} onChange={(e) => setDetailMateriality(e.target.value)} placeholder="Materiality threshold" style={inputStyle()} />
                <input value={detailAbsVarianceMin} onChange={(e) => setDetailAbsVarianceMin(e.target.value)} placeholder="Absolute variance min" style={inputStyle()} />
                <div style={{ position: "relative" }}>
                  <Search size={16} color="#94a3b8" style={{ position: "absolute", left: 12, top: 12 }} />
                  <input value={detailSearch} onChange={(e) => setDetailSearch(e.target.value)} placeholder="Search fund, account, comment..." style={{ ...inputStyle(), paddingLeft: 36 }} />
                </div>
              </div>
            </div>

            <div style={cardStyle()}>
              <div style={sectionHeaderStyle()}>Detailed Rec</div>
              <div style={sectionBodyStyle()}>
                <DetailRecTable rows={filteredDetailRows} comments={comments} drafts={drafts} onDraftChange={onDraftChange} onSave={onSave} />
              </div>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
